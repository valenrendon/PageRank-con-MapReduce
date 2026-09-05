# Plan de implementación — PageRank sobre MapReduce

**Spec:** `pagerank-mapreduce`

> Cada tarea referencia los criterios de aceptación de `requirements.md` que la
> justifican. Marcar una tarea como completa solo cuando esos criterios estén
> verificados por un test o por una corrida real.

---

- [ ] 1. Preparar el esqueleto del proyecto y las constantes
  - Crear `pagerank.py` con el docstring de módulo que explique las tres
    decisiones de diseño (bucle externo, mensaje `STRUCT`, masa colgante en el
    driver).
  - Definir `DAMPING = 0.85`, `MAX_ITER = 50`, `EPSILON = 1e-6`,
    `TAG_RANK = "RANK"`, `TAG_STRUCT = "STRUCT"`.
  - Importar únicamente `sys`, `time` y `from mapreduce_framework import mapreduce`.
  - _Requisitos: 6.1, 6.3_

- [ ] 2. Implementar la carga y normalización del grafo
- [ ] 2.1 Escribir `load_graph(path)`
  - Parsear líneas `nodo: vecino1 vecino2 ...`, saltando comentarios y vacías.
  - Registrar como dangling los nodos sin nada a la derecha de `:`.
  - Recorrer los vecinos referenciados y crear entrada vacía para los que no
    tengan línea propia.
  - _Requisitos: 1.1, 1.2, 1.3, 1.4_
- [ ] 2.2 Escribir `in_degrees(graph)` y `count_edges(graph)`
  - `in_degrees` debe incluir con valor 0 los nodos que no reciben enlaces.
  - _Requisitos: 1.6_
- [ ] 2.3 Escribir los tests de carga
  - Verificar contra `web_graph_sample.txt`: 8 nodos, `E == 12`, `graph["E"] == []`.
  - _Requisitos: 1.1, 1.3, 1.6, 9.7_

- [ ] 3. Implementar el contexto de iteración
  - Crear el diccionario de módulo `_CTX` con `N`, `d` y `dangling_mass`, y el
    contador `_EMITTED` con `rank` y `struct`.
  - Escribir `_set_iteration_context(n, d, dangling_mass)` que además reinicie
    los contadores.
  - Comentar por qué es una variable de módulo y no un parámetro: el framework
    fija las firmas y no admite argumentos extra.
  - _Requisitos: 3.2, 4.2, 8.1_

- [ ] 4. Implementar la fase MAP
- [ ] 4.1 Escribir `pagerank_mapper(item)`
  - Emitir siempre `(node_id, (TAG_STRUCT, adjacency))`, incluso para dangling.
  - Si hay vecinos, emitir `(vecino, (TAG_RANK, rank / len(adjacency)))` por cada uno.
  - Incrementar `_EMITTED` en cada `yield`.
  - _Requisitos: 2.1, 2.2, 2.3, 2.4, 2.5, 8.1_
- [ ] 4.2 Escribir los tests unitarios del mapper
  - Un `STRUCT` por nodo, dirigido a sí mismo, con la adyacencia intacta.
  - Un `RANK` por arista, con la porción correcta.
  - Un dangling emite solo `STRUCT`.
  - _Requisitos: 2.1, 2.2, 2.4, 9.7_

- [ ] 5. Implementar la fase REDUCE
- [ ] 5.1 Escribir `pagerank_reducer(key, values)`
  - Recorrer **toda** la lista separando por etiqueta; no asumir orden.
  - Aplicar `(1-d)/N + d*(suma + D/N)` y retornar `(rank_nuevo, adjacency)`.
  - _Requisitos: 3.1, 3.2, 3.3, 3.4, 3.5, 4.3_
- [ ] 5.2 Escribir el test de preservación de adyacencia
  - Tras una pasada completa de `mapreduce()`, cada nodo conserva su lista de
    vecinos exacta, y el conjunto de claves de salida es igual al de entrada.
  - _Requisitos: 3.3, 2.2, 9.7_

- [ ] 6. Implementar el driver iterativo
- [ ] 6.1 Escribir `run_pagerank(graph, d, max_iter, epsilon, verbose)`
  - Inicializar ranks en `1/N`.
  - Por iteración: calcular `D`, fijar contexto, llamar a `mapreduce()` una vez,
    realimentar con `list(result.items())`.
  - Calcular la norma L1 fuera de `mapreduce()` y cortar si baja de `epsilon`.
  - Acumular `iterations`, `converged`, `l1_history`, `times`,
    `pairs_per_iteration`, `sum_history`.
  - _Requisitos: 4.1, 4.2, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 8.2, 8.3_
- [ ] 6.2 Escribir los tests de convergencia
  - Converge antes del tope; la L1 decrece monótonamente; respeta `max_iter`.
  - Sobre `web_graph_sample.txt`: 26 iteraciones y orden `A > C > B > F > D`.
  - _Requisitos: 5.4, 5.5, 9.1, 9.7_

- [ ] 7. Verificar las invariantes con tests dedicados
- [ ] 7.1 Test del grafo trivial con valores calculados a mano
  - Cadena `A→B→C` con `C` dangling; comparar contra las cuentas manuales, no
    contra la salida del programa.
  - _Requisitos: 9.1_
- [ ] 7.2 Test del ciclo simétrico
  - `A→B→C→A` ⇒ los tres ranks valen `1/3` exacto; converge en la primera vuelta.
  - _Requisitos: 9.2_
- [ ] 7.3 Tests de dangling nodes
  - Un único nodo dangling ⇒ rank `1,0`; dos dangling aislados ⇒ `0,5` cada uno.
  - Test de contraste: sin redistribuir la masa, la suma se fuga por debajo de 0,75.
  - _Requisitos: 4.4, 4.5, 9.3_
- [ ] 7.4 Test de la invariante de suma en cada iteración
  - Sobre tres grafos distintos, comprobar `Σ ranks = 1,0` en **todas** las
    vueltas, no solo al final.
  - _Requisitos: 4.4, 9.4_
- [ ] 7.5 Test de contraste del mensaje `STRUCT`
  - Definir un `mapper_sin_struct` local y afirmar el modo de fallo **real**:
    tras la 1ª pasada nadie conserva adyacencia y desaparecen los nodos sin
    inlinks; tras la 2ª el estado queda vacío.
  - _Requisitos: 9.5_
- [ ] 7.6 Test del volumen de shuffle
  - Los pares emitidos son exactamente `N + E` y constantes entre iteraciones.
  - _Requisitos: 2.6, 8.1, 9.6_

- [ ] 8. Implementar el reporte por consola
- [ ] 8.1 Escribir `report(graph, ranks, stats, top)`
  - Resumen (N, E, dangling, iteraciones, L1, `Σ ranks`, pares, tiempos) y tabla
    top-N con in-degree y posición por in-degree.
  - No usar `print_results()` del framework con el estado completo.
  - _Requisitos: 7.1, 7.2, 7.3_
- [ ] 8.2 Escribir el `main` y la CLI
  - Ruta del grafo posicional con `web_graph_large.txt` por defecto, y `--top N`.
  - _Requisitos: 7.4_

- [ ] 9. Ejecutar y recolectar las mediciones del análisis
  - Correr los tres grafos y capturar iteraciones, pares y tiempos.
  - Medir el ahorro de un combiner: global y simulando 2, 4, 8, 16, 64 y 256 splits.
  - Medir la distribución de in-degree del grafo grande (máximo, promedio,
    mediana, percentiles) para el apartado de data skew.
  - Barrer `epsilon` de `1e-4` a `1e-10` y registrar en qué iteración cruza la L1.
  - _Requisitos: 8.1, 8.2, 8.3_

- [ ] 10. Escribir `ANALYSIS.md` con las cifras medidas
  - Volumen de shuffle, ubicación y ahorro del combiner, data skew, conexión con
    Spark, top-15 contrastado con in-degree, y explicación de por qué la
    correlación no es perfecta.
  - Documentar la discrepancia de iteraciones (16 con `epsilon = 1e-6` frente a
    las ~24 de la referencia, que corresponden a `1e-9`).
  - _Requisitos: 8.1, 8.2, 8.3_

- [ ] 11. (Opcional) Stretch: figura de escalamiento
  - Escribir `grafica_analisis.py` con `matplotlib`, aislado del resto.
  - Cronometrar subgrafos inducidos de 1.000 a 10.000 nodos reportando el
    **mínimo** de varias corridas, y proyectar a 10× y 100×.
  - _Requisitos: 6.4_

- [ ] 12. Escribir `AI_LOG.md`
  - Registrar prompts, lo que la IA generó mal y qué se corrigió, con detalle
    suficiente para reproducir cada error.
  - _Requisitos: (entregable del curso, sin criterio EARS asociado)_

- [ ] 13. Verificación final antes de entregar
  - `diff` contra el `mapreduce_framework.py` original: debe ser idéntico.
  - `grep` de los imports: solo `sys`, `time` y el framework en `pagerank.py`.
  - Suite completa de tests en verde.
  - Corrida limpia de `web_graph_large.txt` con `Σ ranks = 1,0`.
  - _Requisitos: 6.1, 6.2, 6.4, 9.7_ 
