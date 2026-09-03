"""
PageRank sobre el framework MapReduce de los labs.

Autor: Valentina Rendón Claro
Módulo: 01-mapreduce / 01-pure-python

Implementa PageRank usando exclusivamente `mapreduce_framework.mapreduce()`
(sin modificarlo) y la biblioteca estándar de Python.

Idea central del diseño (ver DESIGN.md):
  - El mapper emite DOS tipos de mensaje por nodo:
      RANK   -> hacia cada vecino, con la porción de rank que le cede.
      STRUCT -> hacia sí mismo, con su propia lista de adyacencia.
    El mensaje STRUCT es lo que hace que la estructura del grafo sobreviva
    la pasada: sin él, en la iteración 2 nadie tendría vecinos.
  - La masa de los dangling nodes se calcula en el driver (un barrido O(N))
    y se reparte algebraicamente como D/N dentro del reducer, en vez de
    emitir N mensajes por cada dangling.
  - El bucle de iteración y el criterio de convergencia (norma L1) viven
    en run_pagerank(), FUERA de mapreduce().

Uso:
    python3 pagerank.py                       # corre sobre web_graph_large.txt
    python3 pagerank.py web_graph_sample.txt
    python3 pagerank.py web_graph_medium.txt --top 15
"""

import sys
import time

from mapreduce_framework import mapreduce

# --------------------------------------------------------------------------
# Constantes por defecto
# --------------------------------------------------------------------------

DAMPING = 0.85       # factor de damping exigido por el enunciado
MAX_ITER = 50        # tope duro de iteraciones
EPSILON = 1e-6       # umbral de convergencia sobre la norma L1

# Etiquetas de los dos tipos de mensaje que emite el mapper.
TAG_RANK = "RANK"
TAG_STRUCT = "STRUCT"

# --------------------------------------------------------------------------
# Contexto de la iteración
# --------------------------------------------------------------------------
# El framework fija las firmas mapper(item) y reducer(key, values): no hay
# forma de pasarle parámetros extra. Pero el reducer necesita tres valores
# globales de la iteración (N, d y la masa colgante D). Se dejan en variables
# de módulo que el driver actualiza ANTES de cada llamada a mapreduce().
# No es estado escondido: es el equivalente a la "configuración del job" que
# en Hadoop se distribuye a todas las tareas antes de arrancar la pasada.

_CTX = {
    "N": 0,               # número total de nodos
    "d": DAMPING,         # damping factor
    "dangling_mass": 0.0, # suma de los ranks de los nodos sin enlaces salientes
}

# Contador de pares (clave, valor) emitidos en la pasada actual. Sirve para
# reportar el volumen real de shuffle en ANALYSIS.md.
_EMITTED = {"rank": 0, "struct": 0}


def _set_iteration_context(n, d, dangling_mass):
    """Fija las constantes que el reducer lee durante la pasada."""
    _CTX["N"] = n
    _CTX["d"] = d
    _CTX["dangling_mass"] = dangling_mass
    _EMITTED["rank"] = 0
    _EMITTED["struct"] = 0


# --------------------------------------------------------------------------
# Carga del grafo
# --------------------------------------------------------------------------

def load_graph(path):
    """
    Lee un archivo con formato `nodo: vecino1 vecino2 ...` y devuelve
    un diccionario {node_id: [vecinos]}.

    Reglas de normalización:
      - Las líneas vacías y las que empiezan con '#' se ignoran.
      - Una línea sin nada a la derecha de ':' es un dangling node -> [].
      - Si un nodo aparece SOLO como vecino de otro y nunca tiene línea
        propia, se le crea una entrada con adyacencia vacía. Sin esto, N
        quedaría mal contado y habría ranks fluyendo hacia claves que no
        existen como ítems.
    """
    graph = {}
    referenced = set()

    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            node_id, _, rest = line.partition(":")
            node_id = node_id.strip()
            neighbors = rest.split()
            graph[node_id] = neighbors
            referenced.update(neighbors)

    # Nodos citados como vecinos pero sin línea propia -> dangling.
    for node_id in referenced:
        if node_id not in graph:
            graph[node_id] = []

    return graph


def in_degrees(graph):
    """Cuenta cuántos enlaces entrantes recibe cada nodo."""
    counts = {node_id: 0 for node_id in graph}
    for neighbors in graph.values():
        for neighbor in neighbors:
            counts[neighbor] += 1
    return counts


def count_edges(graph):
    """Número total de aristas E del grafo."""
    return sum(len(neighbors) for neighbors in graph.values())


# --------------------------------------------------------------------------
# MAP
# --------------------------------------------------------------------------

def pagerank_mapper(item):
    """
    Recibe UN ítem `(node_id, (rank, adjacency))` y emite pares (clave, valor).

    Emite dos tipos de mensaje:

      1. STRUCT -> clave = el propio node_id, valor = ("STRUCT", adjacency).
         Se emite SIEMPRE, incluso para dangling nodes y para nodos sin
         enlaces entrantes. Esto cumple dos funciones:
           a) transporta la adyacencia hasta el reducer del propio nodo,
              para que la estructura del grafo sobreviva la pasada;
           b) garantiza que TODO nodo aparezca como clave en el shuffle,
              de modo que el diccionario de salida tenga las mismas N
              claves que la entrada y ningún nodo se pierda por no recibir
              enlaces.

      2. RANK -> clave = cada vecino, valor = ("RANK", rank / out_degree).
         Es la porción de rank que este nodo cede a cada uno de sus vecinos.
         Un dangling node no emite ninguno: no tiene a quién repartirle.
         Su masa se trata globalmente en el reducer (ver _CTX).
    """
    node_id, (rank, adjacency) = item

    # 1) Mensaje de estructura: el nodo se manda su propia adyacencia.
    _EMITTED["struct"] += 1
    yield (node_id, (TAG_STRUCT, adjacency))

    # 2) Mensajes de rank: reparto equitativo entre los vecinos.
    if adjacency:
        share = rank / len(adjacency)
        for neighbor in adjacency:
            _EMITTED["rank"] += 1
            yield (neighbor, (TAG_RANK, share))


# --------------------------------------------------------------------------
# REDUCE
# --------------------------------------------------------------------------

def pagerank_reducer(key, values):
    """
    Recibe la clave de un nodo y la lista de valores que el shuffle agrupó
    para esa clave: exactamente un mensaje STRUCT y cero o más RANK.

    Devuelve `(rank_nuevo, adjacency)` — la MISMA estructura que el valor de
    un ítem de entrada, que es lo que permite realimentar la iteración
    siguiente sin transformaciones adicionales.

    Fórmula aplicada:

        rank_nuevo(P) = (1-d)/N + d * ( Σ contribuciones + D/N )

    donde D es la masa colgante total de la iteración actual, calculada por
    el driver. El término D/N es la redistribución uniforme del rank de los
    dangling nodes; es lo que mantiene Σ ranks = 1.0 exactamente.

    Nota: no se asume ningún orden dentro de `values`. El shuffle del
    framework preserva el orden de emisión, pero el reducer recorre la
    lista entera separando por etiqueta, así que da igual dónde caiga el
    mensaje STRUCT.
    """
    adjacency = []
    contribution_sum = 0.0

    for tag, payload in values:
        if tag == TAG_STRUCT:
            adjacency = payload
        else:  # TAG_RANK
            contribution_sum += payload

    n = _CTX["N"]
    d = _CTX["d"]
    dangling_share = _CTX["dangling_mass"] / n

    new_rank = (1.0 - d) / n + d * (contribution_sum + dangling_share)
    return (new_rank, adjacency)


# --------------------------------------------------------------------------
# Driver: iteración y convergencia (FUERA de mapreduce)
# --------------------------------------------------------------------------

def run_pagerank(graph, d=DAMPING, max_iter=MAX_ITER, epsilon=EPSILON,
                 verbose=False):
    """
    Ejecuta PageRank iterando sobre `mapreduce()`.

    El framework hace una sola pasada Map -> Shuffle -> Reduce y no sabe nada
    de iteraciones. El bucle vive acá, en código Python, sin tocar el
    framework.

    Args:
        graph: dict {node_id: [vecinos]}
        d: damping factor
        max_iter: tope de iteraciones
        epsilon: umbral de convergencia sobre la norma L1
        verbose: imprime el progreso iteración por iteración

    Returns:
        (ranks, stats) donde
          ranks: dict {node_id: rank_final}
          stats: dict con iterations, converged, l1_history, times,
                 pairs_per_iteration y sum_history
    """
    n = len(graph)
    if n == 0:
        return {}, {"iterations": 0, "converged": True, "l1_history": [],
                    "times": [], "pairs_per_iteration": [], "sum_history": []}

    # Inicialización uniforme: todos los nodos arrancan con 1/N.
    state = [(node_id, (1.0 / n, neighbors))
             for node_id, neighbors in graph.items()]

    stats = {"iterations": 0, "converged": False, "l1_history": [],
             "times": [], "pairs_per_iteration": [], "sum_history": []}

    for iteration in range(1, max_iter + 1):
        started = time.perf_counter()

        previous = {node_id: rank for node_id, (rank, _) in state}

        # --- trabajo del driver ANTES de la pasada -------------------------
        # Masa colgante: suma de los ranks de los nodos sin enlaces salientes.
        # Es un barrido O(N). Se calcula acá y no en el mapper porque emitirla
        # como mensajes costaría N pares por cada dangling node.
        dangling_mass = sum(rank for _, (rank, adjacency) in state
                            if not adjacency)
        _set_iteration_context(n, d, dangling_mass)

        # --- una única pasada del framework, sin modificarlo ---------------
        result = mapreduce(state, pagerank_mapper, pagerank_reducer)
        state = list(result.items())

        emitted = _EMITTED["rank"] + _EMITTED["struct"]

        # --- criterio de convergencia, FUERA de mapreduce() ----------------
        current = {node_id: rank for node_id, (rank, _) in state}
        l1 = sum(abs(current[node_id] - previous[node_id])
                 for node_id in current)

        elapsed = time.perf_counter() - started

        stats["iterations"] = iteration
        stats["l1_history"].append(l1)
        stats["times"].append(elapsed)
        stats["pairs_per_iteration"].append(emitted)
        stats["sum_history"].append(sum(current.values()))

        if verbose:
            print(f"  iter {iteration:>3} | L1 = {l1:.3e} | "
                  f"Σ = {sum(current.values()):.12f} | "
                  f"pares = {emitted:,} | {elapsed:.3f}s")

        if l1 < epsilon:
            stats["converged"] = True
            break

    return current, stats


# --------------------------------------------------------------------------
# Reporte
# --------------------------------------------------------------------------

def report(graph, ranks, stats, top=15):
    """Imprime el resumen de la corrida y el top-N contrastado con in-degree."""
    n = len(graph)
    e = count_edges(graph)
    dangling = sum(1 for neighbors in graph.values() if not neighbors)
    indeg = in_degrees(graph)

    print()
    print("=" * 72)
    print(f"  Nodos: {n:,} | Aristas: {e:,} | Dangling: {dangling:,}")
    print(f"  Iteraciones: {stats['iterations']} "
          f"({'convergió' if stats['converged'] else 'tope alcanzado'}) | "
          f"L1 final: {stats['l1_history'][-1]:.3e}")
    print(f"  Σ ranks: {sum(ranks.values()):.12f}")
    print(f"  Pares por iteración: {stats['pairs_per_iteration'][0]:,} "
          f"| Total movido: {sum(stats['pairs_per_iteration']):,}")
    print(f"  Tiempo total: {sum(stats['times']):.2f}s "
          f"| Promedio por iteración: "
          f"{sum(stats['times']) / len(stats['times']):.3f}s")
    print("=" * 72)

    ranking = sorted(ranks.items(), key=lambda pair: pair[1], reverse=True)
    by_indeg = sorted(indeg.items(), key=lambda pair: pair[1], reverse=True)
    indeg_position = {node_id: position
                      for position, (node_id, _) in enumerate(by_indeg, 1)}

    print(f"\n  TOP-{top} POR PAGERANK")
    print(f"  {'#':>3}  {'nodo':<10} {'pagerank':>12} {'in-deg':>8} "
          f"{'pos in-deg':>11}")
    print("  " + "-" * 48)
    for position, (node_id, rank) in enumerate(ranking[:top], 1):
        print(f"  {position:>3}  {node_id:<10} {rank:>12.8f} "
              f"{indeg[node_id]:>8} {indeg_position[node_id]:>11}")
    print()


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv):
    path = "web_graph_large.txt"
    top = 15

    args = list(argv[1:])
    if args and not args[0].startswith("--"):
        path = args.pop(0)
    if "--top" in args:
        top = int(args[args.index("--top") + 1])

    print(f"\nCargando {path} ...")
    graph = load_graph(path)

    print(f"Ejecutando PageRank (d={DAMPING}, epsilon={EPSILON}, "
          f"max_iter={MAX_ITER}) ...\n")
    ranks, stats = run_pagerank(graph, verbose=True)

    report(graph, ranks, stats, top=top)
    return ranks, stats


if __name__ == "__main__":
    main(sys.argv)
