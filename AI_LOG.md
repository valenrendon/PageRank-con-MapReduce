# AI_LOG.md — Bitácora de uso de IA

**Autor(es):** Valentina Rendón Claro  **Fecha:** 31 de agosto de 2026

**Asistente usado:** Claude (Anthropic), en sesiones de trabajo sobre los archivos
del proyecto.

Esta bitácora registra lo que pedí, lo que la IA produjo mal o incompleto, y qué
tuve que corregir. Está ordenada cronológicamente. Los errores están descritos
con el detalle suficiente para reproducirlos, porque varios de ellos son el tipo
de error que **no rompe el programa** — y ese es justamente el punto.

---

## Fase 1 — Diseño (Entrega A)

### 1.1 Prompt inicial

> "Implementa PageRank usando exclusivamente el framework `mapreduce_framework.py`
> de los labs y Python estándar. El reto no es la matemática sino hacerla encajar
> en el paradigma: `mapreduce()` hace una sola pasada, pero PageRank necesita
> muchas. Además hay que preservar la estructura del grafo entre iteraciones y
> manejar los nodos sin enlaces salientes."

Le pasé el enunciado completo y la plantilla antes de pedir nada, precisamente
para que no resolviera el problema equivocado.

### 1.2 Lo que salió bien de entrada

El esquema de dos mensajes (`RANK` hacia los vecinos, `STRUCT` hacia sí mismo)
apareció en la primera respuesta y es correcto. No lo cuento como mérito de la
IA: es el patrón canónico de PageRank sobre MapReduce y está en cualquier libro
de texto. Lo que sí tuve que evaluar yo fue si ese patrón encajaba con **este**
framework en particular, que es lo que el resto de la bitácora documenta.

### 1.3 Error 1 — Diseñar contra un framework que no habíamos leído

**Qué pasó.** Escribimos todo el `DESIGN.md` antes de tener el archivo
`mapreduce_framework.py` a la vista. La IA asumió las firmas a partir del
enunciado y, en particular, asumió que `mapreduce()` **devuelve un diccionario**.
Toda la estrategia de realimentación (`state = list(result.items())`) depende de
ese supuesto.

**Por qué es un problema.** Si el framework hubiera devuelto una lista de pares,
la línea de realimentación habría estado mal y el diseño habría prometido algo
que no compila. Es un supuesto silencioso: nada en el documento avisaba que era
un supuesto.

**Qué corregí.** Exigí que el documento declarara explícitamente el supuesto y
qué cambiaría si resultaba falso, en vez de presentarlo como un hecho. Después
subí el archivo real y lo verificamos:
`def mapreduce(...) -> dict` — el supuesto era correcto, pero **eso lo supimos
después, no antes**. También verificamos que la fase MAP hace
`mapped.extend(mapper(item))`, lo que confirma que un generador con `yield` es
válido, y que el shuffle usa `defaultdict(list)`, cuyo orden de valores no nos
afecta porque el reducer separa por etiqueta.

**Lección.** Un diseño que depende de una API que no leíste es una hipótesis, no
un diseño. Hay que marcarlo como tal.

### 1.4 Error 2 — La IA declaró en el documento que ya había programado

**Qué pasó.** El borrador del `DESIGN.md` cerraba con un párrafo titulado
"Validación del diseño ya ejecutada" que decía: *"implementé el esqueleto de §2
contra el framework sin modificarlo y lo corrí sobre `web_graph_sample.txt`"*.

**Por qué es un problema.** El enunciado dice en §5: *"La Entrega A se revisa y
se te da feedback antes de que programes. No se acepta código sin diseño previo
aprobado."* Entregar un documento de diseño que anuncia que el código ya está
escrito contradice el proceso que la tarea evalúa.

**Qué corregí.** Reescribí el cierre para que dijera lo que efectivamente
aportaba —que verifiqué la aritmética del esquema a mano sobre el grafo de 8
nodos y fijé los valores esperados— sin declarar implementación.

**Lección.** La IA optimiza por "sonar completo". Yo tengo que optimizar por
"cumplir el proceso que me están evaluando". No son lo mismo.

### 1.5 Error 3 — Copiar una cifra del enunciado sin contarla

**Qué pasó.** El anexo del `DESIGN.md` decía `N = 8, E = 11` para
`web_graph_sample.txt`. Ese 11 salió de la tabla del enunciado, que dice
"~11 aristas".

**Por qué es un problema.** El grafo tiene **12** aristas
(A:2 + B:1 + C:1 + D:2 + E:0 + F:4 + G:1 + H:1 = 12). El enunciado usaba el
símbolo "~" — era una aproximación, y la IA la copió como si fuera un dato
exacto.

**Cómo lo detecté.** Cuando `pagerank.py` imprimió su resumen apareció
`Aristas: 12` y no coincidía con mi propio documento.

**Qué corregí.** Corregí el `DESIGN.md` a `E = 12` y agregué un test
(`test_carga_el_grafo_de_ocho_nodos_del_enunciado`) que verifica
`count_edges(graph) == 12` contra el archivo real, para que la cifra quede
anclada al dato y no a mi memoria.

**Lección.** Un número copiado de un enunciado no es un número verificado.

---

## Fase 2 — Implementación (Entrega B)

### 2.1 Error 4 — La descripción del fallo era incorrecta (el hallazgo más importante)

**Qué pasó.** El `DESIGN.md` §3 respondía a la pregunta "¿qué pasa si quitas el
mensaje `STRUCT`?" así:

> *"En la iteración 2 el mapper recibiría ítems sin lista de vecinos ⇒ todos los
> nodos se comportarían como dangling, nadie repartiría nada, y todos los ranks
> colapsarían al mismo valor 1/N. El algoritmo devolvería una distribución
> uniforme."*

Escribí un test de contraste para demostrarlo (`mapper_sin_struct`), con esta
aserción:

```python
finales = [rank for _, (rank, _) in state]
self.assertAlmostEqual(max(finales), min(finales), places=9)
```

**El test no falló por la razón esperada: reventó.**

```
ValueError: max() arg is an empty sequence
```

**Qué encontré al investigar.** Tracé el estado iteración por iteración y el
comportamiento real es **peor** que el descrito:

```
estado inicial : ['A', 'B', 'C', 'D']
tras iteración 1: claves=['A','B','C'] | adyacencias=[[], [], []]
tras iteración 2: claves=[]  ->  el estado quedó VACÍO
```

Es decir:

1. En la **iteración 1** los nodos sin enlaces entrantes (aquí `D`) nunca
   aparecen como clave del shuffle y **desaparecen del estado de inmediato**.
   Los que sobreviven quedan con adyacencia `[]`.
2. En la **iteración 2**, como ningún nodo tiene vecinos, el mapper no emite
   **un solo par**. `mapreduce()` devuelve `{}` y el grafo deja de existir.

Nunca se llega al escenario de "todos los ranks valen 1/N": el grafo se destruye
antes.

**Qué corregí.**

- Reescribí `DESIGN.md` §3 con la secuencia real de fallo (pérdida de nodos en la
  iteración 1, colapso total en la 2), en lugar de la versión intuitiva.
- Reescribí el test como
  `test_sin_struct_el_grafo_se_destruye_en_dos_iteraciones`, que ahora afirma lo
  que de verdad ocurre: comprueba que tras la primera pasada nadie conserva
  adyacencia y que `D` ya no está, y que tras la segunda el estado es `[]`.

**Lección — y es la que más me sirve para la sustentación.** La IA produjo una
explicación *plausible* del fallo, no la *verificada*. Sonaba bien, era coherente,
y era falsa. Lo único que la delató fue escribir un test que intentara
demostrarla. Si hubiera entregado el diseño sin implementarlo, ese párrafo
incorrecto habría pasado sin que nadie lo notara.

### 2.2 Error 5 — Incompatibilidad con `print_results()` del framework

**Qué pasó.** El framework trae un helper `print_results(results, ...)` que hace
`sorted(results.items(), key=lambda x: x[1], reverse=True)`. La primera versión
del código lo usaba para mostrar el top-15.

**Por qué es un problema.** Nuestros valores son tuplas `(rank, adjacency)`. Al
ordenar por `x[1]`, Python compara tuplas: si dos ranks empatan pasa a comparar
**listas de vecinos**, lo cual no falla pero produce un desempate arbitrario y
sin sentido, e imprime la adyacencia completa junto a cada rank.

**Qué corregí.** Escribí mi propia función `report()` que construye un
diccionario plano `{node_id: rank}` y además contrasta cada nodo con su
in-degree, que es lo que el enunciado pide en §9. No modifiqué el framework.

### 2.3 Error 6 — Discrepancia en el número de iteraciones

**Qué pasó.** El enunciado dice que sobre `web_graph_large.txt` el algoritmo
converge en ~24 iteraciones y mueve ~1,75 millones de pares. Mi implementación
convergió en **16** iteraciones y 1.171.120 pares.

**La tentación.** Lo fácil habría sido cambiar el epsilon hasta que diera 24 y
no decir nada.

**Qué hice en cambio.** Barrí el umbral y medí en qué iteración cruza la L1 cada
valor de epsilon:

| epsilon | Iteración | Pares totales |
| --- | ---: | ---: |
| 1e-6 (el que fija el enunciado en §4) | 16 | 1.171.120 |
| 1e-9 | 24 | **1.756.680** ≈ 1,75 M |

Los 1.756.680 pares reproducen exactamente el "~1,75 millones" del enunciado, lo
que muestra que la cifra de referencia se calculó con `epsilon = 1e-9`, más
estricto que el `1e-6` que el propio enunciado especifica en §4. Mi
implementación respeta lo que pide §4, así que reporta 16, y documenté la
diferencia en `ANALYSIS.md` §6.4 con la tabla completa.

**Lección.** Cuando mi resultado no coincide con la referencia, la respuesta no
es ajustar parámetros hasta que coincida: es entender de dónde sale la
diferencia. En este caso el que tenía la inconsistencia era el enunciado.

### 2.4 Error 7 — Artefacto en la gráfica del stretch

**Qué pasó.** La primera versión de la figura de escalamiento dibujaba la recta
de ajuste desde 1.000 pares. Como el ajuste tiene intercepto negativo
(`t = −3,66 ms + 1,68 × pares/1000`), en ese rango el tiempo predicho es negativo
y en un eje logarítmico eso produjo un segmento vertical espurio.

**Qué corregí.** Acoté el rango de la extrapolación a la zona donde el ajuste es
físicamente válido, y agregué en `ANALYSIS.md` §6.2 la advertencia de que la
proyección a 10× y 100× es **optimista**, porque el throughput medido cae de
749k a 594k pares/s entre 1.000 y 10.000 nodos (−21 %) por presión de caché.

---

## Fase 3 — Uso de la IA como revisora

Además de generar, usé la IA para auditar. Dos casos que valieron la pena:

**Contraste con otro diseño.** Le pedí que comparara mi enfoque con el de una
compañera, que sí emite la masa colgante desde el mapper. La comparación
cuantificada (73.195 vs 3.073.195 pares por iteración) es la que terminó
convirtiéndose en el argumento central de `DESIGN.md` §4 y `ANALYSIS.md` §1.4.
Aquí la IA fue útil porque le pedí **números**, no opinión.

**Limpieza del documento.** El borrador del `DESIGN.md` mezclaba diseño con
comentarios de proceso ("verifiqué que...", "estado de esta entrega...") que iban
dirigidos a mí, no al lector del documento. Los saqué todos antes de entregar.

---

## Fase 4 — Spec-driven development en Kiro

**Por qué.** El curso pide trabajar con spec-driven development. Convertí el
diseño ya aprobado en un spec formal: `requirements.md` con 9 requerimientos y 40
criterios de aceptación en notación EARS, un `design.md` técnico, y `tasks.md`
con 13 tareas que referencian cada criterio. Más tres *steering files*
(`product.md`, `tech.md`, `structure.md`) que codifican las restricciones del
enunciado y que Kiro lee en cada interacción. Todo versionado en `.kiro/`.

### 4.1 Verificación de que el steering se aplica

Antes de ejecutar nada le pregunté a Kiro qué restricciones tenía el proyecto.
Respondió correctamente: solo biblioteca estándar, `matplotlib` confinado a
`grafica_analisis.py`, imports permitidos en `pagerank.py` limitados a `sys`,
`time` y el framework, y `mapreduce_framework.py` intocable.

Eso confirma la utilidad real del formato: las restricciones del enunciado dejan
de depender de que yo las repita en cada prompt.

### 4.2 Ejecución de tareas — Kiro verificó en vez de generar

Al ejecutar la tarea 1 (esqueleto y constantes), Kiro detectó que `pagerank.py`
ya existía y cumplía: docstring con las tres decisiones de diseño, solo los
imports permitidos, las cinco constantes definidas. Lo compiló con `py_compile`
para confirmarlo y marcó la tarea como completa.

No es el resultado que esperaba —esperaba que generara código— pero es honesto y
tiene valor: el spec sirvió como **checklist de conformidad** sobre una
implementación existente.

### 4.3 El hallazgo: un requisito ambiguo, no un error de código

Le pedí que auditara la implementación contra los tres criterios que sostienen
el diseño, citando líneas:

> Verifica que `pagerank.py` cumpla los criterios de aceptación 2.2, 4.2 y 4.4 de
> `requirements.md`. Para cada uno, cita la línea exacta del código que lo
> satisface o señala si no se cumple.

Resultado:

| Criterio | Veredicto |
| --- | --- |
| **2.2** — el `STRUCT` se emite incondicionalmente | ✅ el `yield` está antes del `if adjacency:` |
| **4.2** — la masa colgante se calcula en el driver | ✅ barrido `O(N)` antes de `mapreduce()`; el mapper no emite nada relacionado |
| **4.4** — `Σ ranks = 1,0` en cada iteración | ⚠️ **se cumple matemáticamente, pero no se verifica en el código** |

El tercero es el hallazgo. Kiro señaló que mi criterio decía *"el sistema deberá
mantener Σ ranks = 1,0"*, y que eso admite dos lecturas: garantía matemática
—que la fórmula del reducer sí da— o verificación activa en tiempo de ejecución,
que no existía. La suma se registraba en `stats["sum_history"]` y los tests la
comprobaban (criterio 9.4), pero nada detenía una corrida si se rompía.

**El problema no estaba en el código: estaba en cómo escribí el requisito.**

**Qué corregí.** Agregué una guardia explícita en `run_pagerank()`, después de
calcular los ranks de cada iteración:

```python
total = sum(current.values())
if abs(total - 1.0) > 1e-10:
    raise AssertionError(
        f"Invariante de suma rota en la iteración {iteration}: "
        f"Σ ranks = {total!r} (se esperaba 1.0 ± 1e-10)")
```

Verifiqué que la guardia realmente dispara: introduje una fuga deliberada en el
reducer (multiplicar el rank por 0,9) y la corrida se detuvo en la iteración 1
con `Σ ranks = 0.8999999999999999`. Con el código correcto, los 29 tests siguen
pasando y el grafo grande converge igual en 16 iteraciones.

Con eso el criterio 4.4 deja de ser interpretable: la invariante ya no es solo
una propiedad teórica del papel, es una condición que el programa hace cumplir.

### 4.4 Lo que me llevo del formato

El arco completo fue: **escribí el spec → lo usé para auditar mi propio código →
la auditoría encontró que un requisito era ambiguo → precisé el requisito y
endurecí el código.** Ninguna de esas cuatro etapas habría ocurrido pidiéndole
código a una IA y revisándolo después.

Escribir en EARS obliga a una precisión incómoda pero productiva. Dos criterios
tuvieron que quedar mucho más explícitos que en el `DESIGN.md`: el 2.2 (el
`STRUCT` se emite *incondicionalmente* — la implementación intuitiva es ponerlo
dentro del `if adjacency:`, que es justo el error que destruye el grafo) y el 4.2
(la masa colgante **no** se emite desde el mapper — sin esa negación explícita, la
lectura natural lleva a la implementación de 3 millones de pares por iteración).

---

## Balance honesto

**Dónde la IA aportó de verdad:** velocidad para explorar alternativas y
cuantificarlas (los conteos de pares con y sin combiner, el barrido de epsilon,
la simulación de splits), y estructura para redactar.

**Dónde falló:** en todo lo que requería verificar contra la realidad en vez de
producir algo plausible. Los errores 3, 4 y 5 son del mismo tipo — una cifra
copiada sin contar, una explicación coherente pero falsa, una incompatibilidad de
tipos que nadie miró. Ninguno de los tres habría hecho fallar el programa.

**Lo que aprendí sobre el método:** los errores no aparecieron leyendo el código
generado, que se veía bien. Aparecieron cuando escribí tests que intentaban
demostrar afirmaciones concretas y cuando comparé números contra el enunciado. El
test que reveló el error 4 fue precisamente el que yo había escrito para
*confirmar* lo que el diseño decía. Terminó refutándolo, y esa es la parte del
trabajo que la IA no podía hacer por mí: decidir qué había que verificar.

