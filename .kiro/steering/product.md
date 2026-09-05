---
inclusion: always
---

# Producto: PageRank sobre MapReduce

## Qué es esto

Trabajo académico del módulo **01-mapreduce / 01-pure-python**. Implementa el
algoritmo PageRank sobre el framework `mapreduce_framework.py` provisto por el
curso.

**Autora:** Valentina Rendón Claro

## El problema real

PageRank es iterativo. El framework hace **una sola pasada** Map→Shuffle→Reduce
y el mapper recibe **un ítem a la vez**. Todo el ejercicio consiste en resolver
ese desajuste, no en la aritmética del algoritmo (que es trivial).

Tres decisiones de diseño sostienen la solución:

1. **El bucle de iteración vive fuera del framework**, en `run_pagerank()`.
2. **La estructura del grafo se preserva con un mensaje `STRUCT`** que cada nodo
   se emite a sí mismo. Sin él, el grafo se destruye en la segunda iteración.
3. **La masa de los dangling nodes se calcula en el driver** (`O(N)`) y se
   reparte algebraicamente en el reducer, en vez de emitir N mensajes por cada
   nodo colgante.

## Cómo se evalúa

El código es solo el 30 % de la nota. El resto evalúa el razonamiento de diseño,
el análisis de costo, los casos de prueba, la bitácora de uso de IA y una
**defensa oral** en la que se pide justificar una decisión de diseño elegida al
azar.

**Implicación para cualquier cambio en este repositorio:** una solución que
funcione pero que la autora no pueda explicar es peor que una solución más
simple. Ante dos alternativas, prefiere siempre la que se pueda defender
hablando.

## Datasets

| Archivo | Nodos | Aristas | Dangling | Uso |
| --- | ---: | ---: | ---: | --- |
| `web_graph_sample.txt` | 8 | 12 | 1 | Verificable a mano |
| `web_graph_medium.txt` | 1.000 | 6.136 | 30 | Desarrollo |
| `web_graph_large.txt` | 10.000 | 63.195 | 300 | **Dataset oficial** |
