# DESIGN.md — PageRank con MapReduce

**Autor:** Valentina Rendón Claro  **Fecha:** 27 de agosto de 2026

**Módulo:** 01-mapreduce / 01-pure-python · **Framework:** `mapreduce_framework.py` (sin modificar) · **Damping:** d = 0.85

---

## 1. Representación de los datos

El grafo completo se representa como una **lista de ítems**, un ítem por nodo. Cada ítem es una tupla de dos posiciones que empaqueta **el estado dinámico (el rank) junto con la estructura estática (la adyacencia)**. Esa fusión es deliberada: es lo que después permitirá que la estructura sobreviva la pasada (§3).

```python
# Ítem de entrada a mapreduce(): (node_id, (rank, adjacency))
#   node_id   : str   -> identificador del nodo, ej. "A" o "P00042"
#   rank      : float -> rank actual del nodo en la iteración k
#   adjacency : list[str] -> IDs de los nodos a los que enlaza (lista vacía = dangling)

("A", (0.125, ["B", "C"]))
("E", (0.125, []))            # dangling node: sin enlaces salientes

# El estado completo de la iteración k es:
state_k = [("A", (0.125, ["B","C"])), ("B", (0.125, ["C"])), ..., ("H", (0.125, ["A"]))]

# Llamada:
#   result = mapreduce(state_k, pagerank_mapper, pagerank_reducer)  -> dict {node_id: (rank_nuevo, adjacency)}
#   state_k1 = list(result.items())                                  -> misma forma que state_k  => iterable
```

**Invariante de forma (clave del diseño):** la salida del `reducer` tiene exactamente la misma estructura que el valor de un ítem de entrada, `(rank, adjacency)`. Gracias a esto la salida de `mapreduce()` se re-alimenta como entrada de la siguiente pasada sin ninguna transformación adicional más allá de `list(dict.items())`.

**Normalización al cargar (`load_graph`)**: si un nodo aparece únicamente como *vecino* en la lista de otro y nunca como línea propia del archivo, se le crea un ítem con `adjacency = []`. Sin esto tendríamos ranks fluyendo hacia claves que no existen como ítems y `N` quedaría mal contado. En los tres datasets provistos todos los nodos tienen línea propia, pero la defensa es barata y hace la carga robusta.

---

## 2. Esquema clave-valor por fase

### MAP — ¿qué emite el mapper por cada nodo?

El mapper recibe **un ítem a la vez**, `(node_id, (rank, adjacency))`, y emite **dos tipos de mensajes distintos**, discriminados por una etiqueta explícita en la primera posición del valor:

| Tipo de mensaje | Clave | Valor | Propósito | Cuántos por ítem |
| --- | --- | --- | --- | --- |
| **Contribución de rank** (`RANK`) | `vecino` (cada destino en `adjacency`) | `("RANK", rank / len(adjacency))` | Transporta la porción de rank que este nodo cede a cada nodo que enlaza | `out_degree(nodo)` (0 si es dangling) |
| **Estructura** (`STRUCT`) | `node_id` (el nodo **a sí mismo**) | `("STRUCT", adjacency)` | Hace que la lista de adyacencia atraviese el shuffle y llegue al reducer del propio nodo | exactamente **1**, siempre |

```python
def pagerank_mapper(item):
    node_id, (rank, adjacency) = item
    # 1) mensaje de estructura: el nodo se envía su propia adyacencia
    yield (node_id, ("STRUCT", adjacency))
    # 2) mensajes de rank: reparto equitativo entre los vecinos
    if adjacency:                                   # los dangling no emiten RANK
        share = rank / len(adjacency)
        for neighbor in adjacency:
            yield (neighbor, ("RANK", share))
```

Justificación de estas decisiones:

- **Etiqueta explícita en tupla, no truco de tipos.** Se podría distinguir "es `float` ⇒ es rank / es `list` ⇒ es estructura", pero eso acopla la semántica al tipo de dato y se rompe en cuanto quiera agregar un tercer tipo de mensaje. La etiqueta `"RANK"` / `"STRUCT"` es autodocumentada y hace el `reducer` legible.
- **Los dangling emiten `STRUCT` pero no emiten `RANK`.** No tienen a quién repartirle. Su masa se trata globalmente (§4).
- **El mensaje `STRUCT` es incondicional.** Incluso un nodo sin inlinks y sin outlinks (rank cero de contribuciones) aparece como clave en el shuffle. Esto garantiza que **el conjunto de claves de salida sea idéntico al de entrada**: ningún nodo desaparece del estado por no recibir enlaces.

### SHUFFLE — ¿qué queda agrupado por clave?

El framework agrupa todos los valores emitidos con la misma clave. Para un nodo `P`, el reducer recibe una lista con **exactamente un `STRUCT` y cero o más `RANK`**:

```
key = "C"
values = [ ("STRUCT", ["A"]),        # <- emitido por el propio C, siempre presente, exactamente 1
           ("RANK", 0.0625),         # <- de A  (rank 0.125 / out-degree 2)
           ("RANK", 0.1250),         # <- de B  (rank 0.125 / out-degree 1)
           ("RANK", 0.0625),         # <- de D  (rank 0.125 / out-degree 2)
           ("RANK", 0.03125) ]       # <- de F  (rank 0.125 / out-degree 4)
```

El número de `RANK` que llega a `P` es su **in-degree**. Este hecho es exactamente el origen del *data skew* que analizo en `ANALYSIS.md`: en el grafo grande el hub más enlazado recibe ~55-60 mensajes mientras el promedio recibe ~6.

Volumen del shuffle por iteración: **N mensajes `STRUCT` + E mensajes `RANK`** = `N + E`. Para `web_graph_large.txt`: 10.000 + 63.195 ≈ **73.000 pares por iteración**.

### REDUCE — ¿qué retorna el reducer?

Separa los valores por etiqueta, suma las contribuciones, recupera la adyacencia del mensaje `STRUCT` y aplica la fórmula de PageRank con damping y con la corrección por masa colgante:

```python
def pagerank_reducer(key, values):
    adjacency = []
    contrib_sum = 0.0
    for tag, payload in values:
        if tag == "STRUCT":
            adjacency = payload            # estructura recuperada
        else:                              # tag == "RANK"
            contrib_sum += payload         # suma de contribuciones entrantes
    new_rank = (1 - D_FACTOR) / N + D_FACTOR * (contrib_sum + DANGLING_MASS / N)
    return (new_rank, adjacency)           # <-- MISMA forma que el valor de un ítem de entrada
```

`N`, `D_FACTOR` y `DANGLING_MASS` son constantes de la iteración actual: el driver las fija **antes** de cada llamada a `mapreduce()` (§4 y §5). El reducer sigue siendo puro respecto a sus argumentos `(key, values)`, tal como exige la firma del framework.

---

## 3. Preservación de la estructura del grafo

**El mecanismo es el mensaje `STRUCT`: cada nodo, en cada iteración, se envía su propia lista de adyacencia como un mensaje dirigido a sí mismo.**

La adyacencia viaja por el mismo canal que los datos —clave, shuffle, reducer— en lugar de guardarse aparte. Como la clave de ese mensaje es el propio `node_id`, el shuffle lo deposita en el mismo grupo que las contribuciones de rank que llegan a ese nodo, y el reducer puede reconstruir el ítem completo `(rank_nuevo, adjacency)` con la información que tiene en la mano. No hay estado externo, no hay variable global con el grafo, no hay una segunda pasada de "reunir": el grafo se auto-transporta.

**Qué pasaría si NO lo hiciera.** El mapper emite únicamente contribuciones hacia los vecinos. Entonces:

1. El reducer de `P` recibe solo números y no tiene forma de devolver una adyacencia — solo podría retornar el rank.
2. La salida de la iteración 1 sería `{"A": 0.324, "B": 0.111, ...}`: ranks sin estructura.
3. En la iteración 2 el mapper recibiría ítems sin lista de vecinos ⇒ **todos los nodos se comportarían como dangling**, nadie repartiría nada, y todos los ranks colapsarían al mismo valor `(1-d)/N + d/N = 1/N`. El algoritmo devolvería una distribución uniforme: numéricamente "converge", pero a la respuesta equivocada, y de forma silenciosa.
4. Los nodos sin inlinks (`E`, `G`, `H` en el grafo de 8) ni siquiera aparecerían como clave y desaparecerían del estado, haciendo que `N` se encogiera iteración tras iteración.

Es exactamente el error que una implementación ingenua produce y que no se detecta con un test de "corre sin excepciones" — solo lo delata la **invariante de suma** y el test del grafo trivial.

**Alternativa que descarté:** guardar la adyacencia en una variable del driver y volver a unirla al `dict` de ranks después de cada `mapreduce()` (un *join* fuera del framework). Funciona y es más barato en shuffle (ahorra los N mensajes `STRUCT`), pero **rompe el espíritu del ejercicio**: en un MapReduce real y distribuido no existe un "driver" que pueda tener el grafo entero en memoria — es precisamente lo que no cabe. El mensaje `STRUCT` es la solución que escala, y su costo (N de N+E, o sea ~14% del shuffle en el grafo grande) es el precio explícito de no tener estado compartido.

---

## 4. Manejo de dangling nodes

**Decisión: redistribuyo la masa colgante uniformemente entre los N nodos, y la calculo en el driver, no en el mapper.**

Un nodo colgante (`E:` en el grafo de 8) no emite mensajes `RANK`. Si no hago nada más, su rank simplemente **se evapora**: en cada iteración `Σ ranks` baja, y tras suficientes pasadas todo tiende a `(1-d)/N` por nodo. La cadena de Markov deja de ser estocástica.

**Modelo conceptual:** el navegante aleatorio que aterriza en una página sin enlaces salientes no se queda atrapado ni desaparece — teclea una URL al azar. Formalmente, es como si cada dangling enlazara a **todos** los nodos del grafo, incluido él mismo.

**Fórmula final aplicada en el reducer:**

```
D = Σ rank(q)   para todo q con out_degree(q) == 0        (masa colgante de la iteración actual)

rank_nuevo(P) = (1 - d)/N  +  d * ( Σ [rank(Q)/outlinks(Q)]  +  D/N )
                                       Q -> P
```

**Por qué D se calcula en el driver y no se emite desde el mapper.** Si modelara literalmente "el dangling enlaza a todos", el mapper de cada nodo colgante tendría que emitir **N** mensajes `RANK`. En `web_graph_large.txt` eso son 300 dangling × 10.000 nodos = **3.000.000 de pares extra por iteración**, contra los 73.000 del diseño actual: un factor **41×** de shuffle, para transportar una información que es un solo número. La equivalencia matemática es exacta (todos reciben `D/N`), así que el reparto se hace de forma *algebraica* en el reducer y el número `D` se calcula con un barrido `O(N)` en el driver antes de llamar a `mapreduce()`. El volumen resultante (~73.000 pares por iteración) coincide con la escala de referencia indicada en el enunciado.

**Efecto sobre la invariante de suma.** Con esta corrección la suma se conserva exactamente:

```
Σ_P rank_nuevo(P) = N·(1-d)/N + d·( Σ_{Q no dangling} rank(Q) + N·(D/N) )
                  = (1-d) + d·( (1 - D) + D )
                  = (1-d) + d = 1        ✔
```

Verificado a mano sobre `web_graph_sample.txt` (§7): tras la iteración 1 la suma es 1.0 (salvo error de punto flotante ~1e-16). El test de invariante de suma de `test_pagerank.py` va a verificar esto en **cada** iteración, no solo al final — un manejo incorrecto de dangling se delata en la iteración 1.

**Alternativas consideradas y descartadas:**

| Alternativa | Qué implica | Por qué la descarto |
| --- | --- | --- |
| Ignorar la fuga | No corregir nada | `Σ ranks < 1` y decreciente; el ranking relativo se distorsiona hacia los nodos cercanos a los dangling. Rompe el requisito §6.1 del enunciado. |
| Renormalizar al final de cada iteración (dividir todo por `Σ`) | Un barrido extra `O(N)` | Restaura la suma a 1 pero **no** el ranking correcto: reparte la masa perdida en proporción al rank actual (los ricos se hacen más ricos) en vez de uniformemente. Es un parche cosmético sobre la invariante, no una corrección del modelo. |
| Eliminar los dangling del grafo | Podar y recalcular | Cambia el grafo que me pidieron analizar y descarta 300 nodos del dataset oficial, que además deben aparecer en el ranking. |
| Enlazar cada dangling a todos los nodos, literalmente en el mapper | Emitir N mensajes por dangling | Correcto pero inviable: 3M pares/iteración (ver arriba). Misma matemática, 41× el costo. |

---

## 5. Iteración y convergencia

**Dónde vive el bucle.** En `run_pagerank(graph, d=0.85, max_iter=50, epsilon=1e-6)`, una función del driver que **envuelve** a `mapreduce()`. El framework hace una sola pasada Map→Shuffle→Reduce y no sabe nada de iteraciones; el bucle `while` es código Python mío, fuera del framework, que no lo modifica.

```python
def run_pagerank(graph, d=0.85, max_iter=50, epsilon=1e-6):
    N = len(graph)
    state = [(node, (1.0 / N, adj)) for node, adj in graph.items()]   # inicialización uniforme

    for iteration in range(1, max_iter + 1):
        prev = {node: rank for node, (rank, _) in state}

        # --- trabajo del driver ANTES de la pasada: masa colgante O(N) ---
        dangling_mass = sum(rank for _, (rank, adj) in state if not adj)
        set_iteration_context(N=N, d=d, dangling_mass=dangling_mass)   # constantes que lee el reducer

        # --- una única pasada del framework, sin modificarlo ---
        result = mapreduce(state, pagerank_mapper, pagerank_reducer)
        state = list(result.items())

        # --- criterio de convergencia: norma L1, FUERA de mapreduce() ---
        curr = {node: rank for node, (rank, _) in state}
        delta = sum(abs(curr[node] - prev[node]) for node in curr)
        if delta < epsilon:
            break
    return curr, iteration, delta
```

**Criterio de convergencia.** `L1 = Σ_P |rank_{k+1}(P) − rank_k(P)| < epsilon`, con `epsilon = 1e-6`. Uso L1 y no L∞ ni L2 porque L1 es la norma natural sobre una distribución de probabilidad (y los ranks suman 1): mide la masa total de probabilidad que se movió en la pasada, así que un `epsilon` fijo significa lo mismo independientemente del tamaño del grafo. La comprobación vive en el bucle del driver, **fuera** de `mapreduce()`; el framework no tiene forma de expresarla porque cada reducer solo ve su propia clave y no puede comparar contra la iteración anterior.

**Cómo comparo entre iteración N y N+1.** Extraigo dos diccionarios `{node_id: rank}` —`prev` antes de la pasada y `curr` después— y sumo las diferencias absolutas nodo a nodo. Solo guardo los ranks, no la adyacencia, así que el costo de memoria del chequeo es `O(N)` floats y no una copia del grafo. Las claves de `prev` y `curr` son idénticas por construcción (§2: el mensaje `STRUCT` garantiza que ningún nodo se pierda), lo cual también sirve como aserción de sanidad.

**Corte duro.** `max_iter = 50` protege contra un grafo patológico que oscile sin converger. Sobre `web_graph_large.txt` se espera convergencia en ~24 iteraciones, muy por debajo del tope; en mi verificación a mano sobre el grafo de 8 nodos converge en **26** iteraciones con `epsilon = 1e-6`.

---

## 6. Diagrama de una iteración

```
                       ITERACIÓN k  (una sola pasada de mapreduce())
                       ─────────────────────────────────────────────

  DRIVER (fuera del framework)
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  D = Σ rank(q) para q dangling      ──►  DANGLING_MASS   [barrido O(N)]  │
  └──────────────────────────────────────────────────────────────────────────┘
                                    │
   INPUT  state_k                   │ constantes N, d, D visibles al reducer
   ┌──────────────────────────┐     ▼
   │ ("A", (0.125, [B,C]))    │
   │ ("B", (0.125, [C]))      │
   │ ("E", (0.125, []))  dang │
   │ ...                      │
   └────────────┬─────────────┘
                │  el mapper recibe UN ítem a la vez
                ▼
   ══════ MAP ══════════════════════════════════════════════════════════════
     ítem ("A", (0.125, [B,C]))          ítem ("E", (0.125, []))
        │                                    │
        ├─► ("A", ("STRUCT", [B,C]))         └─► ("E", ("STRUCT", []))
        ├─► ("B", ("RANK",  0.0625))              (dangling: 0 mensajes RANK;
        └─► ("C", ("RANK",  0.0625))               su masa ya está en D)

     Emisiones totales por iteración:  N mensajes STRUCT  +  E mensajes RANK
     ══► web_graph_large.txt: 10.000 + 63.195 ≈ 73.000 pares
                │
                ▼
   ══════ SHUFFLE ═══════════════════════════════════════════════════════════
     agrupa por clave  →  cada nodo recibe 1 STRUCT (siempre) + in_degree RANK

        "C" → [ ("STRUCT", ["A"]),                    ← su propia adyacencia
                ("RANK", 0.0625),  ← de A
                ("RANK", 0.1250),  ← de B
                ("RANK", 0.0625),  ← de D
                ("RANK", 0.03125) ]← de F
                │
                ▼
   ══════ REDUCE ════════════════════════════════════════════════════════════
     reducer("C", values):
        adjacency   ← payload del mensaje STRUCT        (estructura preservada)
        contrib_sum ← Σ payloads de los mensajes RANK   = 0.28125
        new_rank    = (1-d)/N + d * (contrib_sum + D/N)
                    = 0.01875 + 0.85*(0.28125 + 0.015625) = 0.27109375
        return (0.27109375, ["A"])       ◄── MISMA FORMA que un valor de entrada
                │
                ▼
   OUTPUT  state_{k+1} = list(result.items())
   ┌──────────────────────────┐
   │ ("A", (0.3242, [B,C]))   │
   │ ("C", (0.2711, [A]))     │      Σ ranks = 1.0  ✔ (invariante)
   │ ...                      │
   └────────────┬─────────────┘
                │
   ══════ CONVERGENCIA (driver, fuera de mapreduce) ═════════════════════════
       L1 = Σ |rank_{k+1}(P) − rank_k(P)|
       ¿L1 < epsilon (1e-6)?
                ├── SÍ  ──► FIN, devolver ranking
                └── NO  ──► k = k+1, realimentar state_{k+1} ──┐
                                                               │
        └──────────────────────────────────────────────────────┘
                       (aquí está el costo que motiva Spark:
                        el grafo COMPLETO se vuelve a emitir en cada vuelta)
```

---

## 7. Anexo — Verificación aritmética a mano (`web_graph_sample.txt`)

Grafo: `A→B,C` · `B→C` · `C→A` · `D→A,C` · `E→` (dangling) · `F→A,B,C,D` · `G→F` · `H→A`.
`N = 8`, `E = 11`, rank inicial `1/8 = 0.125`, `d = 0.85`, `(1−d)/N = 0.01875`, `D = rank(E) = 0.125`, `D/N = 0.015625`.

| Nodo | Contribuciones entrantes | Σ contrib | rank tras iteración 1 |
| --- | --- | --- | --- |
| A | C:0.125 + D:0.0625 + F:0.03125 + H:0.125 | 0.34375 | **0.32421875** |
| B | A:0.0625 + F:0.03125 | 0.09375 | **0.11171875** |
| C | A:0.0625 + B:0.125 + D:0.0625 + F:0.03125 | 0.28125 | **0.27109375** |
| D | F:0.03125 | 0.03125 | **0.05859375** |
| E | — | 0 | **0.03203125** |
| F | G:0.125 | 0.125 | **0.13828125** |
| G | — | 0 | **0.03203125** |
| H | — | 0 | **0.03203125** |
| | | | **Σ = 1.0** ✔ |

Ejemplo de cuenta (nodo A): `0.01875 + 0.85 × (0.34375 + 0.015625) = 0.01875 + 0.85 × 0.359375 = 0.32421875`.

Convergencia esperada del grafo de 8 nodos con `epsilon = 1e-6`: **26 iteraciones**, ranking final `A (0.3497) > C (0.3415) > B (0.1779) > F (0.0388) > D (0.0292) > E = G = H (0.0210)`. Los tres nodos sin inlinks (E, G, H) quedan empatados en el piso `(1−d)/N + d·D/N`, que es lo que predice el modelo. Estos son los valores esperados de los tests del grafo trivial en la Entrega B.
