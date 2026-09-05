# Diseño técnico — PageRank sobre MapReduce

**Spec:** `pagerank-mapreduce`
**Documento de diseño académico asociado:** `DESIGN.md` (raíz del repositorio)

> Este archivo es el diseño **interno del spec**: guía la generación y ejecución
> de tareas dentro de Kiro. El entregable evaluado por el curso es `DESIGN.md` en
> la raíz, que sigue la plantilla del profesor. Los dos son coherentes entre sí,
> pero tienen destinatarios distintos.

---

## 1. Visión general

El framework `mapreduce()` ejecuta **una** pasada Map→Shuffle→Reduce. PageRank
requiere muchas. La arquitectura resuelve ese desajuste con una separación
estricta de responsabilidades:

```
┌─────────────────────────────────────────────────────────────┐
│ DRIVER  (run_pagerank)          — código propio, iterativo  │
│  · inicializa ranks en 1/N                                  │
│  · calcula la masa colgante D          [barrido O(N)]       │
│  · llama a mapreduce() UNA vez por iteración                │
│  · evalúa convergencia con norma L1                         │
│  · decide si continuar                                      │
└───────────────────────────┬─────────────────────────────────┘
                            │  una pasada por vuelta
┌───────────────────────────▼─────────────────────────────────┐
│ FRAMEWORK  (mapreduce_framework.py)   — INTOCABLE           │
│  MAP → SHUFFLE → REDUCE, sin estado entre llamadas          │
└─────────────────────────────────────────────────────────────┘
```

**Regla arquitectónica:** todo lo que necesite ver más de un nodo a la vez, o
recordar algo entre iteraciones, vive en el driver. El mapper y el reducer son
funciones puras respecto de sus argumentos.

---

## 2. Modelo de datos

```python
Item      = tuple[str, tuple[float, list[str]]]   # ("A", (0.125, ["B","C"]))
Mensaje   = tuple[str, tuple[str, Any]]           # ("B", ("RANK", 0.0625))
Estado    = list[Item]
```

**Invariante de forma:** el reducer retorna `(rank, adjacency)`, exactamente el
valor de un ítem de entrada. Por eso la realimentación es
`state = list(result.items())` y nada más.

---

## 3. Componentes

| Componente | Responsabilidad | Archivo |
| --- | --- | --- |
| `load_graph` | Parseo y normalización del archivo a `{nodo: [vecinos]}` | `pagerank.py` |
| `in_degrees`, `count_edges` | Métricas del grafo para el reporte y el análisis | `pagerank.py` |
| `pagerank_mapper` | Emite `STRUCT` + `RANK` por ítem | `pagerank.py` |
| `pagerank_reducer` | Separa por etiqueta, aplica la fórmula | `pagerank.py` |
| `_CTX` / `_set_iteration_context` | Constantes de la iteración visibles al reducer | `pagerank.py` |
| `_EMITTED` | Contador de pares para el análisis de costo | `pagerank.py` |
| `run_pagerank` | Bucle, convergencia, estadísticas | `pagerank.py` |
| `report` | Salida por consola, top-N vs in-degree | `pagerank.py` |
| `medir_escalamiento` | Cronometraje sobre subgrafos inducidos | `grafica_analisis.py` |

### 3.1 El problema del contexto y cómo se resuelve

El framework fija las firmas `mapper(item)` y `reducer(key, values)`: no admite
parámetros extra. Pero el reducer necesita tres valores globales de la iteración
(`N`, `d`, `D`).

**Solución:** variables de módulo en el diccionario `_CTX`, que el driver
actualiza con `_set_iteration_context()` antes de cada llamada.

**Alternativas descartadas:**

- *Closures / `functools.partial`* — más limpio conceptualmente, pero el
  framework recibe la función y la invoca directamente; una closure funcionaría,
  aunque oscurece que el reducer depende de estado externo. La variable de módulo
  lo hace explícito.
- *Recalcular `D` dentro del reducer* — imposible: el reducer solo ve su clave.

Esto no es estado escondido: es el equivalente a la configuración del job que en
Hadoop se distribuye a todas las tareas antes de arrancar la pasada.

---

## 4. Flujo de una iteración

```
state_k ──► [driver: D = Σ rank(dangling)] ──► _set_iteration_context(N, d, D)
                                                        │
                                                        ▼
   ┌──────────────────── mapreduce(state_k, mapper, reducer) ───────────────────┐
   │                                                                           │
   │  MAP      cada ítem ──► 1 STRUCT (a sí mismo) + out_degree RANK (vecinos)  │
   │           total: N + E pares                                              │
   │                                                                           │
   │  SHUFFLE  agrupa por clave ──► [1 STRUCT] + [in_degree × RANK]            │
   │                                                                           │
   │  REDUCE   suma RANK, recupera adyacencia del STRUCT,                      │
   │           rank = (1-d)/N + d·(Σcontrib + D/N)                             │
   │           retorna (rank, adjacency)                                       │
   └───────────────────────────────┬───────────────────────────────────────────┘
                                   ▼
              state_{k+1} = list(result.items())
                                   │
              L1 = Σ|rank_{k+1} − rank_k|  ──►  ¿< epsilon?  ──► sí: fin
                                   │                             no: k+1
                                   └─────────────────────────────┘
```

---

## 5. Decisiones de diseño y sus alternativas

### 5.1 Preservación del grafo — mensaje `STRUCT`

**Decisión:** cada nodo emite su adyacencia dirigida a sí mismo.

**Alternativa descartada:** guardar el grafo en el driver y hacer un *join* con
el diccionario de ranks después de cada pasada. Ahorraría los N mensajes
`STRUCT` (13,7 % del shuffle), pero presupone una máquina capaz de tener el
grafo entero en memoria — justo lo que un MapReduce distribuido no puede
asumir.

**Modo de fallo si se omite** (verificado empíricamente, no supuesto):

1. Iteración 1 → los nodos sin inlinks nunca aparecen como clave y desaparecen
   del estado; los demás quedan con adyacencia `[]`.
2. Iteración 2 → nadie tiene vecinos, el mapper no emite un solo par,
   `mapreduce()` retorna `{}` y el grafo deja de existir.
3. **Nunca se lanza una excepción.** El bucle se queda sin datos.

### 5.2 Masa colgante calculada en el driver

**Decisión:** `D` se computa con un barrido `O(N)` y se aplica algebraicamente
como `D/N` en el reducer.

**Alternativa descartada:** emitir desde el mapper de cada dangling un mensaje a
cada uno de los N nodos. Matemáticamente idéntico; en el grafo grande son
3.073.195 pares por iteración contra 73.195, un factor **42×**, para transportar
un solo `float`.

### 5.3 Etiquetas explícitas en vez de `isinstance`

**Decisión:** `("RANK", float)` / `("STRUCT", list)`.

**Alternativa descartada:** discriminar por tipo. Funciona, pero acopla la
semántica del mensaje a su representación y se rompe al añadir un tercer tipo.

### 5.4 Norma L1 para la convergencia

Los ranks suman 1, es decir, son una distribución de probabilidad. L1 mide la
masa total que se movió en la pasada, así que un `epsilon` fijo significa lo
mismo en cualquier tamaño de grafo. Medido sobre el grafo grande: L1 converge en
16 iteraciones, L∞ en 10 y L2 en 12.

---

## 6. Estrategia de pruebas

| Nivel | Qué verifica |
| --- | --- |
| **Unitario del mapper** | Que emite 1 `STRUCT` y `out_degree` mensajes `RANK`, con la porción correcta |
| **Unitario del reducer** | Que recupera la adyacencia y aplica la fórmula |
| **Valores a mano** | Cadena de 3 nodos y grafo de 8 nodos, con cifras calculadas con lápiz |
| **Simetría** | Ciclo `A→B→C→A` ⇒ los tres ranks valen exactamente `1/3` |
| **Invariante** | `Σ ranks = 1,0` en cada iteración, sobre varios grafos |
| **Contraste (fallo)** | Reproducir la omisión del `STRUCT` y la fuga de dangling, y afirmar el fallo real |
| **Costo** | Que el volumen de shuffle sea `N + E` y constante |

**Principio:** los valores esperados se calculan a mano o se derivan de una
propiedad matemática. Un test que compara el programa contra su propia salida no
prueba nada.

---

## 7. Riesgos conocidos

| Riesgo | Mitigación |
| --- | --- |
| Un cambio rompe la invariante de suma sin fallar visiblemente | `TestInvarianteDeSuma` corre sobre cada iteración |
| Alguien "mejora" el reducer asumiendo que el `STRUCT` viene primero | El reducer recorre la lista completa; hay test de orden |
| Se pasa el estado completo a `print_results()` del framework | Documentado en `tech.md`; `report()` es la vía correcta |
| La lista `mapped` se materializa entera (9,0 MB a 10.000 nodos) | Limitación conocida del framework; documentada en `ANALYSIS.md` §1.5 |
| Cronometraje ruidoso en máquinas compartidas | Se reporta el mínimo de 21 corridas, no el promedio |
