"""
Genera la figura `escalamiento.png` que acompaña a ANALYSIS.md §6.

Autor: Valentina Rendón Claro

NOTA SOBRE DEPENDENCIAS: este script usa matplotlib, que es una librería
externa. Es el ÚNICO archivo del proyecto que lo hace, y se usa exclusivamente
para dibujar la figura del stretch opcional. El algoritmo (`pagerank.py`) y los
tests (`test_pagerank.py`) usan solo el framework de los labs y la biblioteca
estándar, como exige el enunciado §4. Si no se tiene matplotlib, todo lo demás
del proyecto corre igual; solo no se regenera la imagen.

Uso:
    python3 grafica_analisis.py
"""

import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import pagerank as pr
from mapreduce_framework import mapreduce

SURF = "#fcfcfb"; INK = "#0b0b0b"; INK2 = "#52514e"
BLUE = "#2a78d6"; GRID = "#e3e2df"; GRAY = "#a8a7a2"

GRAFO = "web_graph_large.txt"
TAMANOS = [1000, 2000, 3000, 4000, 6000, 8000, 10000]
REPETICIONES = 7   # se reporta el mínimo de estas corridas (ver medir_escalamiento)


def medir_escalamiento(graph):
    """Cronometra una iteración sobre subgrafos inducidos de tamaño creciente."""
    nodos = list(graph)
    medidas = []
    for objetivo in TAMANOS:
        seleccion = set(nodos[:objetivo])
        sub = {k: [v for v in graph[k] if v in seleccion] for k in seleccion}
        n = len(sub)
        state = [(k, (1.0 / n, v)) for k, v in sub.items()]
        tiempos = []
        for _ in range(REPETICIONES):
            masa = sum(r for _, (r, a) in state if not a)
            pr._set_iteration_context(n, pr.DAMPING, masa)
            inicio = time.perf_counter()
            resultado = mapreduce(state, pr.pagerank_mapper, pr.pagerank_reducer)
            tiempos.append(time.perf_counter() - inicio)
            state = list(resultado.items())
        # Se toma el MÍNIMO, no el promedio: en un cronometraje el ruido del
        # planificador del sistema solo puede sumar tiempo, nunca restarlo, así
        # que la corrida más rápida es la menos contaminada.
        medidas.append((n, pr.count_edges(sub), n + pr.count_edges(sub),
                        min(tiempos)))
    return medidas


def historial_l1(graph, vueltas=30):
    """L1 por iteración, más allá del punto de convergencia, para la figura."""
    _, stats = pr.run_pagerank(graph, max_iter=vueltas, epsilon=0.0)
    return stats["l1_history"]


def ajuste_lineal(xs, ys):
    n = len(xs); sx = sum(xs); sy = sum(ys)
    sxy = sum(x * y for x, y in zip(xs, ys)); sxx = sum(x * x for x in xs)
    b = (n * sxy - sx * sy) / (n * sxx - sx * sx)
    return (sy - b * sx) / n, b


def main():
    graph = pr.load_graph(GRAFO)
    medidas = medir_escalamiento(graph)
    l1 = historial_l1(graph)

    pares = [m[2] for m in medidas]; tiempos = [m[3] for m in medidas]
    a, b = ajuste_lineal(pares, tiempos)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.4))
    fig.patch.set_facecolor(SURF)

    # -- Panel A: escalamiento -------------------------------------------
    ax1.set_facecolor(SURF)
    xs = [9e3, 1e4, 1e5, 1e6, 1e7]
    ax1.plot(xs, [a + b * x for x in xs], "--", linewidth=1.5, color=GRAY,
             zorder=1, label="extrapolación lineal")
    ax1.plot(pares, tiempos, marker="o", markersize=7, linewidth=2, color=BLUE,
             zorder=3, label="medido (subgrafos reales)")
    for factor in (10, 100):
        p = pares[-1] * factor; t = a + b * p
        ax1.plot([p], [t], marker="o", markersize=8, markerfacecolor=SURF,
                 markeredgecolor=BLUE, markeredgewidth=2, zorder=3)
        ax1.annotate(f"{factor}x → {t:.1f} s/iter", (p, t),
                     textcoords="offset points", xytext=(-8, 10), ha="right",
                     fontsize=9, color=INK2)
    ax1.annotate(f"dataset oficial\n{pares[-1]:,} pares · {tiempos[-1]:.2f} s/iter"
                 .replace(",", "."), (pares[-1], tiempos[-1]),
                 textcoords="offset points", xytext=(12, -26), fontsize=9, color=INK)
    ax1.set_xscale("log"); ax1.set_yscale("log")
    ax1.set_xlabel("Pares (clave, valor) movidos por iteración", fontsize=10, color=INK2)
    ax1.set_ylabel("Segundos por iteración", fontsize=10, color=INK2)
    ax1.set_title("El costo por iteración crece lineal con N + E",
                  fontsize=11.5, color=INK, pad=12, loc="left")
    ax1.legend(frameon=False, fontsize=9, loc="upper left", labelcolor=INK2)

    # -- Panel B: convergencia -------------------------------------------
    ax2.set_facecolor(SURF)
    its = list(range(1, len(l1) + 1))
    corte = next(i for i, v in enumerate(l1, 1) if v < pr.EPSILON)
    ax2.plot(its[:corte], l1[:corte], marker="o", markersize=5, linewidth=2,
             color=BLUE, zorder=3, label=f"corrida real (para en la {corte})")
    ax2.plot(its[corte - 1:], l1[corte - 1:], "--", linewidth=1.5, color=GRAY,
             zorder=2, label="continuación si ε fuera menor")
    for eps, etq in [(1e-6, f"ε = 1e-6  →  {corte} iter"),
                     (1e-9, "ε = 1e-9  →  24 iter  (escala del enunciado)")]:
        ax2.axhline(eps, color=GRAY, linewidth=1, linestyle="--", zorder=1)
        ax2.annotate(etq, (1, eps), textcoords="offset points", xytext=(2, 5),
                     fontsize=9, color=INK2)
    ax2.set_yscale("log"); ax2.set_xlim(0.5, 26); ax2.set_ylim(1e-11, 3)
    ax2.set_xlabel("Iteración", fontsize=10, color=INK2)
    ax2.set_ylabel("Norma L1 entre iteraciones", fontsize=10, color=INK2)
    razon = (l1[0] / l1[-1]) ** (1 / (len(l1) - 1))
    ax2.set_title(f"La convergencia es geométrica: L1 cae ~{razon:.1f}x por vuelta"
                  .replace(".", ","), fontsize=11.5, color=INK, pad=12, loc="left")
    ax2.legend(frameon=False, fontsize=9, loc="upper right", labelcolor=INK2)

    for ax in (ax1, ax2):
        ax.grid(True, color=GRID, linewidth=0.8, zorder=0)
        ax.set_axisbelow(True)
        for lado in ("top", "right"): ax.spines[lado].set_visible(False)
        for lado in ("left", "bottom"): ax.spines[lado].set_color(GRID)
        ax.tick_params(colors=INK2, labelsize=9)

    fig.suptitle(f"PageRank sobre MapReduce — escalamiento y convergencia ({GRAFO})",
                 fontsize=12.5, color=INK, x=0.008, ha="left", y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig("escalamiento.png", dpi=170, facecolor=SURF)
    print("escalamiento.png generado")
    print(f"  throughput sostenido: {1/b/1000:.0f}k pares/s")
    print(f"  L1 cae {razon:.2f}x por iteración")


if __name__ == "__main__":
    main()
