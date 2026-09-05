# Requerimientos — PageRank sobre MapReduce

**Autora:** Valentina Rendón Claro
**Spec:** `pagerank-mapreduce`
**Flujo:** Design-First (existe un documento de diseño previo aprobado, `DESIGN.md`)

> Los criterios de aceptación usan notación **EARS** (*Easy Approach to
> Requirements Syntax*) con las palabras clave en español:
> `CUANDO <evento> EL SISTEMA DEBERÁ <comportamiento>` para respuestas a eventos,
> `SI <condición> ENTONCES EL SISTEMA DEBERÁ <comportamiento>` para casos
> condicionales, y `EL SISTEMA DEBERÁ <comportamiento>` para requisitos
> ubicuos.

---

## Requerimiento 1 — Carga y representación del grafo

**Historia de usuario:** Como estudiante que debe correr PageRank sobre tres
datasets distintos, quiero cargar un archivo de grafo a una estructura uniforme,
para que el resto del algoritmo no dependa del formato del archivo.

### Criterios de aceptación

1.1. CUANDO se invoque `load_graph(path)` con un archivo de formato
`nodo: vecino1 vecino2 ...` EL SISTEMA DEBERÁ retornar un diccionario
`{node_id: [vecinos]}` con una entrada por nodo.

1.2. CUANDO una línea del archivo empiece con `#` o esté vacía EL SISTEMA DEBERÁ
ignorarla sin lanzar error.

1.3. CUANDO una línea no tenga nada a la derecha de `:` EL SISTEMA DEBERÁ
registrar ese nodo con lista de adyacencia vacía, es decir, como dangling node.

1.4. SI un nodo aparece únicamente como vecino de otro y nunca tiene línea propia
ENTONCES EL SISTEMA DEBERÁ crearle una entrada con adyacencia vacía, de modo que
`N` quede correctamente contado y ninguna contribución fluya hacia una clave
inexistente.

1.5. EL SISTEMA DEBERÁ representar cada nodo como el ítem
`(node_id, (rank, adjacency))`, empaquetando el estado dinámico y la estructura
estática en el mismo valor.

1.6. CUANDO se invoque `count_edges(graph)` EL SISTEMA DEBERÁ retornar la suma de
los out-degrees, y CUANDO se invoque `in_degrees(graph)` DEBERÁ retornar el
número de enlaces entrantes de cada nodo, incluyendo los nodos con cero.

---

## Requerimiento 2 — Fase MAP: emisión de mensajes

**Historia de usuario:** Como diseñadora del algoritmo, quiero que el mapper
emita dos tipos de mensaje claramente diferenciados, para que la estructura del
grafo sobreviva la pasada además de las contribuciones de rank.

### Criterios de aceptación

2.1. CUANDO el mapper procese un ítem `(node_id, (rank, adjacency))` EL SISTEMA
DEBERÁ emitir exactamente un mensaje `(node_id, ("STRUCT", adjacency))` dirigido
al propio nodo.

2.2. EL SISTEMA DEBERÁ emitir el mensaje `STRUCT` **incondicionalmente**,
incluyendo los dangling nodes y los nodos sin enlaces entrantes, de modo que todo
nodo aparezca como clave del shuffle.

2.3. SI la lista de adyacencia no está vacía ENTONCES EL SISTEMA DEBERÁ emitir,
por cada vecino, un mensaje `(vecino, ("RANK", rank / len(adjacency)))`.

2.4. SI la lista de adyacencia está vacía ENTONCES EL SISTEMA DEBERÁ emitir cero
mensajes `RANK`.

2.5. EL SISTEMA DEBERÁ etiquetar cada mensaje con una cadena explícita
(`"RANK"` / `"STRUCT"`) en lugar de discriminar por el tipo del dato, para no
acoplar la semántica del mensaje a su representación.

2.6. EL SISTEMA DEBERÁ emitir exactamente `N + E` pares por iteración, y ese
volumen DEBERÁ ser idéntico en todas las iteraciones.

---

## Requerimiento 3 — Fase REDUCE: cálculo del nuevo rank

**Historia de usuario:** Como diseñadora del algoritmo, quiero que el reducer
devuelva un valor con la misma forma que la entrada, para poder realimentar la
iteración siguiente sin transformaciones.

### Criterios de aceptación

3.1. CUANDO el reducer reciba la lista de valores de una clave EL SISTEMA DEBERÁ
separarlos por etiqueta, sumando los payloads `RANK` y tomando la adyacencia del
mensaje `STRUCT`.

3.2. EL SISTEMA DEBERÁ calcular el nuevo rank como
`(1 - d)/N + d * (suma_contribuciones + masa_colgante/N)`.

3.3. EL SISTEMA DEBERÁ retornar la tupla `(rank_nuevo, adjacency)`, idéntica en
forma al valor de un ítem de entrada.

3.4. EL SISTEMA DEBERÁ producir el mismo resultado independientemente del orden
en que el shuffle haya colocado los valores dentro de la lista.

3.5. CUANDO una clave no reciba ningún mensaje `RANK` EL SISTEMA DEBERÁ tratar la
suma de contribuciones como cero, sin lanzar error.

---

## Requerimiento 4 — Manejo de dangling nodes

**Historia de usuario:** Como estudiante evaluada por la invariante de suma,
quiero que el rank de los nodos sin enlaces salientes no se fugue del sistema,
para que `Σ ranks` se mantenga en 1,0.

### Criterios de aceptación

4.1. CUANDO comience cada iteración EL SISTEMA DEBERÁ calcular la masa colgante
`D` como la suma de los ranks de todos los nodos con adyacencia vacía.

4.2. EL SISTEMA DEBERÁ calcular `D` en el driver mediante un barrido `O(N)`
**antes** de invocar `mapreduce()`, y NO DEBERÁ emitir esa masa como mensajes
desde el mapper.

4.3. EL SISTEMA DEBERÁ repartir `D` de forma uniforme entre los `N` nodos
mediante el término `D/N` dentro de la fórmula del reducer.

4.4. EL SISTEMA DEBERÁ mantener `Σ ranks = 1,0` (tolerancia `1e-10`) al final de
**cada** iteración, no solamente al converger.

4.5. SI el grafo consiste en un único nodo dangling ENTONCES EL SISTEMA DEBERÁ
asignarle rank `1,0`.

---

## Requerimiento 5 — Iteración y convergencia

**Historia de usuario:** Como estudiante que debe usar un framework de una sola
pasada, quiero que el bucle y el criterio de parada vivan en mi propio código,
para no modificar el framework.

### Criterios de aceptación

5.1. EL SISTEMA DEBERÁ implementar el bucle de iteración dentro de
`run_pagerank(graph, d, max_iter, epsilon)`, invocando `mapreduce()` una vez por
iteración.

5.2. EL SISTEMA DEBERÁ inicializar todos los ranks en `1/N`.

5.3. CUANDO termine una iteración EL SISTEMA DEBERÁ calcular la norma L1
`Σ |rank_nuevo(P) − rank_viejo(P)|` sobre todos los nodos.

5.4. SI la norma L1 es menor que `epsilon` ENTONCES EL SISTEMA DEBERÁ detener el
bucle y marcar la corrida como convergida.

5.5. SI se alcanza `max_iter` sin converger ENTONCES EL SISTEMA DEBERÁ detenerse
y marcar la corrida como no convergida, sin lanzar error.

5.6. EL SISTEMA DEBERÁ realizar la comprobación de convergencia **fuera** de
`mapreduce()`.

5.7. EL SISTEMA DEBERÁ retornar, junto con los ranks, estadísticas de la corrida:
número de iteraciones, historial de L1, tiempos por iteración, pares emitidos por
iteración e historial de sumas.

---

## Requerimiento 6 — Restricciones de dependencias y del framework

**Historia de usuario:** Como estudiante sujeta a las reglas del enunciado,
quiero que la implementación no dependa de nada externo, para que la entrega sea
válida.

### Criterios de aceptación

6.1. EL SISTEMA DEBERÁ usar exclusivamente la biblioteca estándar de Python y
`mapreduce_framework.py`.

6.2. EL SISTEMA NO DEBERÁ modificar `mapreduce_framework.py` bajo ninguna
circunstancia.

6.3. EL SISTEMA DEBERÁ respetar las firmas `mapper(item)` como generador y
`reducer(key, values)`.

6.4. SI se requiere `matplotlib` ENTONCES EL SISTEMA DEBERÁ confinarlo a
`grafica_analisis.py`, y el resto del proyecto DEBERÁ ejecutarse correctamente sin
él.

---

## Requerimiento 7 — Reporte de resultados

**Historia de usuario:** Como estudiante que debe reportar el top-15 contrastado
con el in-degree, quiero una salida legible por consola, para poder pegarla en el
análisis.

### Criterios de aceptación

7.1. CUANDO termine una corrida EL SISTEMA DEBERÁ imprimir N, E, número de
dangling, iteraciones, L1 final, `Σ ranks`, pares por iteración y tiempo total.

7.2. EL SISTEMA DEBERÁ imprimir el top-N por PageRank mostrando, para cada nodo,
su rank, su in-degree y su posición en el ranking por in-degree.

7.3. EL SISTEMA NO DEBERÁ usar `print_results()` del framework con el estado
completo, porque ordenaría comparando listas de adyacencia.

7.4. CUANDO se ejecute desde la línea de comandos EL SISTEMA DEBERÁ aceptar la
ruta del grafo y una opción `--top N`, usando `web_graph_large.txt` por defecto.

---

## Requerimiento 8 — Instrumentación para el análisis de costo

**Historia de usuario:** Como estudiante que debe sustentar cifras de shuffle,
quiero que el programa las mida en vez de estimarlas, para que el análisis sea
verificable.

### Criterios de aceptación

8.1. EL SISTEMA DEBERÁ contar los pares emitidos por el mapper, discriminando
`RANK` de `STRUCT`, y reiniciar el contador al inicio de cada iteración.

8.2. EL SISTEMA DEBERÁ medir el tiempo de reloj de cada iteración.

8.3. EL SISTEMA DEBERÁ registrar el `Σ ranks` de cada iteración, de modo que la
invariante sea auditable a posteriori.

---

## Requerimiento 9 — Cobertura de pruebas

**Historia de usuario:** Como estudiante evaluada en casos de prueba, quiero
tests que verifiquen afirmaciones concretas contra valores calculados a mano,
para que un error silencioso no pase inadvertido.

### Criterios de aceptación

9.1. EL SISTEMA DEBERÁ incluir un test sobre un grafo trivial (cadena de tres
nodos) cuyos valores esperados estén calculados a mano y no copiados de la salida
del programa.

9.2. EL SISTEMA DEBERÁ incluir un test sobre un ciclo `A→B→C→A` que verifique que
los tres ranks son iguales a `1/3` por simetría.

9.3. EL SISTEMA DEBERÁ incluir tests sobre dangling nodes, incluido el caso de un
único nodo dangling.

9.4. EL SISTEMA DEBERÁ verificar la invariante `Σ ranks = 1,0` en **cada**
iteración y no solo al final.

9.5. EL SISTEMA DEBERÁ incluir un test de contraste que reproduzca el error de
omitir el mensaje `STRUCT` y documente el modo de fallo real.

9.6. EL SISTEMA DEBERÁ verificar que el volumen de shuffle es exactamente
`N + E`.

9.7. CUANDO se ejecute la suite completa EL SISTEMA DEBERÁ pasar todos los tests
sin errores ni advertencias.
