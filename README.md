# PageRank con MapReduce 

**Autora:** Valentina Rendón Claro
**Curso:** Módulo 01-mapreduce / 01-pure-python
**Framework:** `mapreduce_framework.py` del curso — sin mrjob, sin librerías externas

Implementación del algoritmo PageRank sobre un framework MapReduce de una sola
pasada. El reto no es la aritmética, sino hacer que un algoritmo **iterativo**
quepa en un paradigma diseñado para **una** pasada Map→Shuffle→Reduce.



## Cómo correrlo

```bash
python3 pagerank.py                          # dataset oficial (10.000 nodos)
python3 pagerank.py web_graph_sample.txt     # grafo de 8 nodos, verificable a mano
python3 pagerank.py web_graph_medium.txt --top 20

python3 -m unittest test_pagerank            # 29 tests
```

Requiere solo Python 3 y la biblioteca estándar.
*(`grafica_analisis.py` usa `matplotlib` únicamente para regenerar la figura del
stretch opcional; el resto del proyecto corre sin él.)*

---

## Las tres decisiones de diseño

**1. El bucle de iteración vive fuera del framework.**
`mapreduce()` hace una sola pasada. El `while` y el criterio de convergencia
(norma L1 < 1e-6) están en `run_pagerank()`, en código propio. El framework no se
modificó.

**2. La estructura del grafo se preserva con un mensaje `STRUCT`.**
Cada nodo emite su propia lista de adyacencia dirigida a sí mismo, para que
atraviese el shuffle y llegue al reducer junto con las contribuciones de rank.
Sin ese mensaje el grafo **desaparece del estado en la segunda iteración**, sin
lanzar ninguna excepción — verificado en
`test_sin_struct_el_grafo_se_destruye_en_dos_iteraciones`.

**3. La masa de los dangling nodes se calcula en el driver.**
Un barrido `O(N)` antes de cada pasada, repartido algebraicamente como `D/N`
dentro del reducer. Emitirla desde el mapper costaría **3.073.195 pares por
iteración en lugar de 73.195** — 42× más tráfico para transportar un solo número.

---

## Resultados sobre `web_graph_large.txt`

| | |
| --- | ---: |
| Nodos / aristas / dangling | 10.000 / 63.195 / 300 |
| Iteraciones hasta converger (ε = 1e-6) | 16 |
| Pares por iteración | 73.195 = 10.000 STRUCT + 63.195 RANK |
| Pares totales movidos | 1.171.120 |
| Σ ranks | 1,000000000000 |
| Tiempo total | ~2,4 s |
| Correlación PageRank vs in-degree | Pearson 0,866 · Spearman 0,917 |

![Escalamiento y convergencia](escalamiento.png)

---

## Estructura del repositorio

| Archivo | Contenido |
| --- | --- |
| [`DESIGN.md`](DESIGN.md) | **Entrega A** — documento de diseño |
| [`pagerank.py`](pagerank.py) | Implementación completa |
| [`test_pagerank.py`](test_pagerank.py) | 29 casos de prueba |
| [`ANALYSIS.md`](ANALYSIS.md) | Análisis de costo: shuffle, combiner, data skew, conexión con Spark |
| [`AI_LOG.md`](AI_LOG.md) | Bitácora de uso de IA |
| [`grafica_analisis.py`](grafica_analisis.py) | Genera `escalamiento.png` (stretch opcional) |
| `mapreduce_framework.py` | Provisto por el curso — **sin modificar** |
| `web_graph_*.txt` | Los tres grafos de prueba |
| `.kiro/` | Spec-driven development: requirements (EARS), design, tasks y steering files |

---

## Sobre el proceso

El trabajo se estructuró como **spec-driven development**: los requisitos se
escribieron primero en notación EARS (*cuando ocurre X, el sistema deberá hacer
Y*), lo que los hace verificables — el criterio 4.4, "al final de cada iteración
Σ ranks deberá ser 1,0", se traduce literalmente en `TestInvarianteDeSuma`.

El spec completo está en [`.kiro/specs/pagerank-mapreduce/`](.kiro/specs/pagerank-mapreduce/)
y las restricciones del enunciado quedaron codificadas en
[`.kiro/steering/tech.md`](.kiro/steering/tech.md).

El uso de IA está documentado con honestidad en [`AI_LOG.md`](AI_LOG.md),
incluyendo los siete errores que hubo que corregir. El más importante: la IA
describió mal el modo de fallo al omitir el mensaje `STRUCT`, y el error se
detectó porque el test escrito para *confirmar* esa afirmación falló.
