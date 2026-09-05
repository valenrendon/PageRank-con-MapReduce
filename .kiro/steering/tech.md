---
inclusion: always
---

# Restricciones técnicas — NO NEGOCIABLES

Estas reglas vienen del enunciado de la tarea. Violar cualquiera invalida la
entrega, así que aplican a todo código generado en este repositorio.

## Dependencias

- **Solo la biblioteca estándar de Python.** Prohibido `networkx`, `numpy`,
  `mrjob`, `pandas`, `scipy` y cualquier otro paquete externo.
- **Única excepción:** `grafica_analisis.py` usa `matplotlib`, exclusivamente
  para dibujar la figura del stretch opcional. Ningún otro archivo puede
  importarlo, y el proyecto debe correr completo sin él.
- Los imports permitidos en `pagerank.py` son: `sys`, `time` y
  `from mapreduce_framework import mapreduce`. Si necesitas algo más, primero
  pregunta.

## El framework

- **`mapreduce_framework.py` NO SE MODIFICA.** Ni una línea, ni un comentario.
  Se usa tal cual lo entregó el curso.
- Firmas que hay que respetar:
  - `mapreduce(data, mapper, reducer) -> dict` — retorna `{clave: valor_reducido}`
  - `mapper(item)` — recibe **un** ítem, hace `yield (clave, valor)`
  - `reducer(key, values) -> valor_reducido`
- El framework **no tiene combiners**. No inventes uno: el análisis de costo
  discute dónde iría *si existiera*, pero el código no lo implementa.
- `print_results()` del framework ordena por `x[1]`. Nuestros valores son tuplas
  `(rank, adjacency)`, así que **no se le pasa el estado completo** — se usa
  `report()` de `pagerank.py`, que arma un diccionario plano `{nodo: rank}`.

## Parámetros del algoritmo

- Damping `d = 0.85` (lo fija el enunciado).
- Convergencia: **norma L1** `Σ|rank_nuevo − rank_viejo| < epsilon`, con
  `epsilon = 1e-6`. La comprobación va en el driver, **fuera** de `mapreduce()`.
- Tope `max_iter = 50`.

## Invariantes que ningún cambio puede romper

1. **`Σ ranks = 1.0` en TODAS las iteraciones**, no solo al final. Si se rompe,
   el manejo de dangling nodes está mal.
2. **El diccionario de salida tiene las mismas N claves que la entrada.** Ningún
   nodo desaparece por no recibir enlaces.
3. **El reducer devuelve `(rank, adjacency)`** — la misma forma que el valor de
   un ítem de entrada. Es lo que permite realimentar la iteración.
4. **El volumen de shuffle es exactamente `N + E` pares por iteración.**

## Estilo

- Comentarios y docstrings **en español**; identificadores en inglés
  (`node_id`, `adjacency`, `pagerank_mapper`), consistente con `DESIGN.md`.
- Cada decisión no obvia lleva un comentario que explique **por qué**, no qué.
