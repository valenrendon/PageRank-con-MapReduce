---
inclusion: always
---

# Estructura del repositorio

```
pagerank/
├── DESIGN.md                 # ENTREGABLE A — documento de diseño (25 %)
├── pagerank.py               # ENTREGABLE B — implementación (30 %)
├── test_pagerank.py          # ENTREGABLE B — casos de prueba (10 %)
├── ANALYSIS.md               # ENTREGABLE B — análisis de costo (15 %)
├── AI_LOG.md                 # ENTREGABLE B — bitácora de IA (10 %)
├── grafica_analisis.py       # stretch opcional (única fuente que usa matplotlib)
├── escalamiento.png          # figura generada, referenciada por ANALYSIS.md
├── mapreduce_framework.py    # PROVISTO POR EL CURSO — NO TOCAR
├── web_graph_sample.txt      # 8 nodos
├── web_graph_medium.txt      # 1.000 nodos
├── web_graph_large.txt       # 10.000 nodos — dataset oficial
└── .kiro/
    ├── steering/             # contexto permanente del proyecto
    └── specs/pagerank-mapreduce/
        ├── requirements.md
        ├── design.md
        └── tasks.md
```

## Cuidado con los dos "design"

Son archivos distintos y no hay que confundirlos:

| Archivo | Qué es |
| --- | --- |
| `DESIGN.md` (raíz) | **El entregable evaluado.** Sigue la plantilla del curso, en español, dirigido al profesor. |
| `.kiro/specs/pagerank-mapreduce/design.md` | El diseño técnico interno del spec de Kiro. Guía la generación de tareas. |

El segundo puede evolucionar libremente. El primero **solo se modifica con una
razón explícita**, porque ya fue entregado y aprobado.

## Convenciones

- Un archivo por responsabilidad; nada de scripts sueltos en la raíz.
- Los archivos temporales de exploración se borran antes de empaquetar.
- `escalamiento.png` se regenera con `python3 grafica_analisis.py`; no se edita
  a mano.
