"""
Casos de prueba para pagerank.py

Autor: Valentina Rendón Claro

Ejecutar con:
    python3 test_pagerank.py           # salida detallada
    python3 -m unittest test_pagerank  # salida estándar de unittest

Los tests están agrupados por lo que verifican y cada clase explica POR QUÉ
ese caso es relevante. Los valores esperados de los grafos triviales están
calculados a mano (ver DESIGN.md §7), no copiados de la salida del programa:
un test que compara el programa contra sí mismo no prueba nada.
"""

import unittest

from mapreduce_framework import mapreduce
import pagerank as pr


D = pr.DAMPING          # 0.85
EPS = pr.EPSILON        # 1e-6


# ==========================================================================
# 1. GRAFO TRIVIAL — resultado calculable a mano
# ==========================================================================

class TestGrafoTrivial(unittest.TestCase):
    """
    Cadena de 3 nodos: A -> B -> C, y C es dangling.

    Es el caso más pequeño que ejercita las tres piezas del diseño a la vez:
    reparto de rank, preservación de adyacencia y masa colgante. Los valores
    esperados se calculan a mano abajo, así que si la fórmula del reducer
    está mal el test falla con un número concreto, no con un "no converge".
    """

    def setUp(self):
        self.graph = {"A": ["B"], "B": ["C"], "C": []}

    def test_primera_iteracion_coincide_con_el_calculo_a_mano(self):
        # Cuentas a mano, N = 3, d = 0.85, rank inicial 1/3:
        #   (1-d)/N = 0.15/3 = 0.05
        #   D = rank(C) = 1/3  ->  D/N = 1/9
        #   contribuciones: A recibe 0 | B recibe 1/3 (de A) | C recibe 1/3 (de B)
        #   A = 0.05 + 0.85 * (0     + 1/9) = 0.14444444...
        #   B = 0.05 + 0.85 * (1/3   + 1/9) = 0.42777777...
        #   C = 0.05 + 0.85 * (1/3   + 1/9) = 0.42777777...
        esperado = {
            "A": 0.05 + 0.85 * (0.0 + 1.0 / 9.0),
            "B": 0.05 + 0.85 * (1.0 / 3.0 + 1.0 / 9.0),
            "C": 0.05 + 0.85 * (1.0 / 3.0 + 1.0 / 9.0),
        }

        ranks, _ = pr.run_pagerank(self.graph, max_iter=1, epsilon=0.0)

        for node_id, valor in esperado.items():
            self.assertAlmostEqual(ranks[node_id], valor, places=10,
                                   msg=f"rank de {node_id} tras 1 iteración")

    def test_la_suma_de_la_primera_iteracion_es_exactamente_uno(self):
        ranks, _ = pr.run_pagerank(self.graph, max_iter=1, epsilon=0.0)
        self.assertAlmostEqual(sum(ranks.values()), 1.0, places=12)

    def test_el_orden_final_respeta_la_direccion_de_la_cadena(self):
        # C recibe de B, que recibe de A, que no recibe de nadie.
        # Con la masa colgante repartida, C >= B > A.
        ranks, _ = pr.run_pagerank(self.graph)
        self.assertGreater(ranks["C"], ranks["A"])
        self.assertGreater(ranks["B"], ranks["A"])


# ==========================================================================
# 2. CICLO — todos los ranks deben ser iguales por simetría
# ==========================================================================

class TestCicloSimetrico(unittest.TestCase):
    """
    A -> B -> C -> A.

    Es el mejor test de correctitud "gratis" que existe: la respuesta se
    conoce sin calcular nada. El grafo es simétrico bajo rotación, así que
    los tres ranks TIENEN que ser 1/3 exacto. Cualquier asimetría en el
    reparto, en el damping o en el orden de agregación rompe esta igualdad.
    """

    def setUp(self):
        self.graph = {"A": ["B"], "B": ["C"], "C": ["A"]}

    def test_los_tres_ranks_son_iguales(self):
        ranks, _ = pr.run_pagerank(self.graph)
        self.assertAlmostEqual(ranks["A"], ranks["B"], places=12)
        self.assertAlmostEqual(ranks["B"], ranks["C"], places=12)

    def test_cada_rank_vale_un_tercio(self):
        ranks, _ = pr.run_pagerank(self.graph)
        for node_id in self.graph:
            self.assertAlmostEqual(ranks[node_id], 1.0 / 3.0, places=12)

    def test_converge_en_la_primera_iteracion(self):
        # Arranca en 1/3 y ya está en el punto fijo: la L1 de la primera
        # pasada debe ser ~0, así que converge de inmediato.
        _, stats = pr.run_pagerank(self.graph)
        self.assertTrue(stats["converged"])
        self.assertEqual(stats["iterations"], 1)


# ==========================================================================
# 3. DANGLING NODES
# ==========================================================================

class TestDanglingNodes(unittest.TestCase):
    """
    El caso que rompe las implementaciones ingenuas. Un nodo sin enlaces
    salientes no puede repartir su rank; si no se hace nada, esa masa
    desaparece del sistema y Σ ranks decae iteración tras iteración.
    """

    def test_un_unico_nodo_dangling_conserva_todo_el_rank(self):
        # Un solo nodo, sin enlaces. Su rank tiene que quedar en 1.0:
        #   (1-d)/1 + d * (0 + 1.0/1) = 0.15 + 0.85 = 1.0
        ranks, _ = pr.run_pagerank({"A": []})
        self.assertAlmostEqual(ranks["A"], 1.0, places=12)

    def test_el_dangling_no_emite_mensajes_de_rank(self):
        emitidos = list(pr.pagerank_mapper(("E", (0.125, []))))
        etiquetas = [tag for _, (tag, _) in emitidos]
        self.assertEqual(etiquetas, [pr.TAG_STRUCT],
                         "un dangling solo debe emitir su mensaje STRUCT")

    def test_la_masa_colgante_se_reparte_por_igual(self):
        # Dos nodos aislados y dangling: por simetría, 0.5 cada uno.
        ranks, _ = pr.run_pagerank({"A": [], "B": []})
        self.assertAlmostEqual(ranks["A"], 0.5, places=12)
        self.assertAlmostEqual(ranks["B"], 0.5, places=12)

    def test_sin_manejo_de_dangling_la_suma_se_fuga(self):
        # Test de contraste: reproduce el error que se quiere evitar.
        # Si NO se redistribuye la masa colgante, la suma cae por debajo de 1.
        graph = {"A": ["B"], "B": ["C"], "C": []}
        n = len(graph)
        rank = {node_id: 1.0 / n for node_id in graph}

        for _ in range(10):
            entrante = {node_id: 0.0 for node_id in graph}
            for origen, vecinos in graph.items():
                if vecinos:
                    porcion = rank[origen] / len(vecinos)
                    for destino in vecinos:
                        entrante[destino] += porcion
            # Fórmula SIN el término D/N — el error deliberado.
            rank = {node_id: (1 - D) / n + D * entrante[node_id]
                    for node_id in graph}

        self.assertLess(sum(rank.values()), 0.75,
                        "sin redistribuir la masa colgante la suma debe fugarse")


# ==========================================================================
# 4. INVARIANTE DE SUMA — en CADA iteración, no solo al final
# ==========================================================================

class TestInvarianteDeSuma(unittest.TestCase):
    """
    Σ ranks debe mantenerse en 1.0 en todas las iteraciones. Verificarlo solo
    al final no sirve: un error de fuga puede compensarse visualmente cuando
    los ranks ya se estabilizaron. La comprobación va iteración por iteración.
    """

    def _verificar_todas_las_iteraciones(self, graph, vueltas=15):
        n = len(graph)
        state = [(node_id, (1.0 / n, vecinos))
                 for node_id, vecinos in graph.items()]

        for numero in range(1, vueltas + 1):
            masa = sum(rank for _, (rank, vecinos) in state if not vecinos)
            pr._set_iteration_context(n, D, masa)
            resultado = mapreduce(state, pr.pagerank_mapper,
                                  pr.pagerank_reducer)
            state = list(resultado.items())
            total = sum(rank for _, (rank, _) in state)
            self.assertAlmostEqual(
                total, 1.0, places=10,
                msg=f"Σ ranks se rompió en la iteración {numero}: {total}")

    def test_invariante_en_el_grafo_de_la_cadena(self):
        self._verificar_todas_las_iteraciones({"A": ["B"], "B": ["C"], "C": []})

    def test_invariante_en_el_grafo_con_varios_dangling(self):
        self._verificar_todas_las_iteraciones({
            "A": ["B", "C"], "B": ["C"], "C": ["A"], "D": [], "E": [], "F": []})

    def test_invariante_en_el_grafo_de_ocho_nodos(self):
        self._verificar_todas_las_iteraciones({
            "A": ["B", "C"], "B": ["C"], "C": ["A"], "D": ["A", "C"],
            "E": [], "F": ["A", "B", "C", "D"], "G": ["F"], "H": ["A"]})

    def test_el_historial_de_sumas_de_run_pagerank_se_mantiene_en_uno(self):
        graph = {"A": ["B", "C"], "B": ["C"], "C": ["A"], "D": []}
        _, stats = pr.run_pagerank(graph)
        for numero, total in enumerate(stats["sum_history"], 1):
            self.assertAlmostEqual(total, 1.0, places=10,
                                   msg=f"iteración {numero}")


# ==========================================================================
# 5. PRESERVACIÓN DE LA ESTRUCTURA DEL GRAFO
# ==========================================================================

class TestPreservacionDelGrafo(unittest.TestCase):
    """
    El mecanismo central del diseño: el mensaje STRUCT. Estos tests verifican
    que la adyacencia sobrevive la pasada, y el último demuestra qué pasa si
    se quita — que es justo la pregunta de la defensa oral.
    """

    def setUp(self):
        self.graph = {"A": ["B", "C"], "B": ["C"], "C": ["A"], "D": []}

    def test_el_mapper_emite_un_struct_por_nodo(self):
        emitidos = list(pr.pagerank_mapper(("A", (0.25, ["B", "C"]))))
        structs = [par for par in emitidos if par[1][0] == pr.TAG_STRUCT]
        self.assertEqual(len(structs), 1)
        self.assertEqual(structs[0][0], "A", "el STRUCT va dirigido a sí mismo")
        self.assertEqual(structs[0][1][1], ["B", "C"])

    def test_el_mapper_emite_un_rank_por_arista(self):
        emitidos = list(pr.pagerank_mapper(("A", (0.4, ["B", "C"]))))
        ranks = [par for par in emitidos if par[1][0] == pr.TAG_RANK]
        self.assertEqual(len(ranks), 2)
        self.assertEqual({par[0] for par in ranks}, {"B", "C"})
        for _, (_, porcion) in ranks:
            self.assertAlmostEqual(porcion, 0.2, places=12)

    def test_la_adyacencia_sale_intacta_del_reducer(self):
        n = len(self.graph)
        pr._set_iteration_context(n, D, 0.25)
        state = [(node_id, (1.0 / n, vecinos))
                 for node_id, vecinos in self.graph.items()]
        resultado = mapreduce(state, pr.pagerank_mapper, pr.pagerank_reducer)

        for node_id, vecinos in self.graph.items():
            self.assertEqual(resultado[node_id][1], vecinos,
                             f"se perdió la adyacencia de {node_id}")

    def test_ningun_nodo_desaparece_aunque_no_reciba_enlaces(self):
        # G y H no reciben enlaces de nadie: solo aparecen como clave gracias
        # a su propio mensaje STRUCT.
        graph = {"A": ["B"], "B": ["A"], "G": ["A"], "H": []}
        n = len(graph)
        pr._set_iteration_context(n, D, 0.25)
        state = [(node_id, (1.0 / n, vecinos))
                 for node_id, vecinos in graph.items()]
        resultado = mapreduce(state, pr.pagerank_mapper, pr.pagerank_reducer)
        self.assertEqual(set(resultado), set(graph))

    def test_sin_struct_el_grafo_se_destruye_en_dos_iteraciones(self):
        """
        Test de contraste, el más importante del archivo.

        Reproduce el error de un mapper que NO emite el mensaje STRUCT y
        documenta exactamente cómo falla, que resultó ser peor de lo que
        sugiere la intuición:

          - Iteración 1: el reducer no recibe ninguna adyacencia, así que
            todos los nodos salen con vecinos = []. Además, los nodos sin
            enlaces entrantes nunca aparecen como clave y DESAPARECEN del
            estado (aquí, D).
          - Iteración 2: como ningún nodo tiene vecinos, el mapper no emite
            un solo par. mapreduce() devuelve un diccionario vacío y el
            grafo deja de existir.

        Lo grave es que en ningún momento se lanza una excepción: el bucle
        simplemente se queda sin datos.
        """
        def mapper_sin_struct(item):
            node_id, (rank, vecinos) = item
            if vecinos:
                porcion = rank / len(vecinos)
                for vecino in vecinos:
                    yield (vecino, (pr.TAG_RANK, porcion))

        n = len(self.graph)
        state = [(node_id, (1.0 / n, vecinos))
                 for node_id, vecinos in self.graph.items()]

        def una_pasada(estado, mapper):
            masa = sum(rank for _, (rank, vecinos) in estado if not vecinos)
            pr._set_iteration_context(n, D, masa)
            return list(mapreduce(estado, mapper, pr.pagerank_reducer).items())

        # Iteración 1: se pierden las adyacencias y el nodo sin inlinks.
        state = una_pasada(state, mapper_sin_struct)
        self.assertTrue(all(vecinos == [] for _, (_, vecinos) in state),
                        "sin STRUCT ningún nodo conserva su adyacencia")
        self.assertNotIn("D", dict(state),
                         "D no recibe enlaces: sin STRUCT desaparece del estado")

        # Iteración 2: no queda nada que emitir.
        state = una_pasada(state, mapper_sin_struct)
        self.assertEqual(state, [],
                         "sin STRUCT el grafo desaparece en la 2ª iteración")

        # Contraste: con el diseño correcto no se pierde ningún nodo y sí
        # hay ranking diferenciado.
        correctos, _ = pr.run_pagerank(self.graph)
        self.assertEqual(set(correctos), set(self.graph))
        self.assertGreater(max(correctos.values()) - min(correctos.values()),
                           0.01, "el diseño correcto debe producir ranking")


# ==========================================================================
# 6. CARGA Y NORMALIZACIÓN DEL GRAFO
# ==========================================================================

class TestCargaDelGrafo(unittest.TestCase):

    def test_carga_el_grafo_de_ocho_nodos_del_enunciado(self):
        graph = pr.load_graph("web_graph_sample.txt")
        self.assertEqual(len(graph), 8)
        self.assertEqual(graph["A"], ["B", "C"])
        self.assertEqual(graph["E"], [], "E es dangling")
        self.assertEqual(pr.count_edges(graph), 12)

    def test_ignora_comentarios_y_lineas_vacias(self):
        graph = pr.load_graph("web_graph_sample.txt")
        self.assertNotIn("#", "".join(graph))

    def test_cuenta_bien_los_grados_de_entrada(self):
        graph = {"A": ["B", "C"], "B": ["C"], "C": [], "D": ["C"]}
        self.assertEqual(pr.in_degrees(graph),
                         {"A": 0, "B": 1, "C": 3, "D": 0})


# ==========================================================================
# 7. CONVERGENCIA
# ==========================================================================

class TestConvergencia(unittest.TestCase):

    def setUp(self):
        self.graph = pr.load_graph("web_graph_sample.txt")

    def test_converge_antes_del_tope_de_iteraciones(self):
        _, stats = pr.run_pagerank(self.graph)
        self.assertTrue(stats["converged"])
        self.assertLess(stats["iterations"], pr.MAX_ITER)

    def test_la_norma_l1_decrece_monotonamente(self):
        _, stats = pr.run_pagerank(self.graph)
        historial = stats["l1_history"]
        for anterior, siguiente in zip(historial, historial[1:]):
            self.assertLess(siguiente, anterior,
                            "la L1 debe decrecer en cada iteración")

    def test_la_l1_final_queda_bajo_epsilon(self):
        _, stats = pr.run_pagerank(self.graph)
        self.assertLess(stats["l1_history"][-1], EPS)

    def test_respeta_el_tope_de_iteraciones(self):
        _, stats = pr.run_pagerank(self.graph, max_iter=3, epsilon=0.0)
        self.assertEqual(stats["iterations"], 3)
        self.assertFalse(stats["converged"])

    def test_el_ranking_del_grafo_de_ocho_coincide_con_el_diseno(self):
        # Valores fijados en DESIGN.md §7, calculados a mano antes de programar.
        ranks, stats = pr.run_pagerank(self.graph)
        self.assertEqual(stats["iterations"], 26)

        orden = [node_id for node_id, _ in
                 sorted(ranks.items(), key=lambda par: par[1], reverse=True)]
        self.assertEqual(orden[:5], ["A", "C", "B", "F", "D"])

        self.assertAlmostEqual(ranks["A"], 0.349716, places=6)
        self.assertAlmostEqual(ranks["C"], 0.341454, places=6)
        self.assertAlmostEqual(ranks["B"], 0.177856, places=6)

        # E, G y H no reciben enlaces: quedan empatados en el piso.
        self.assertAlmostEqual(ranks["E"], ranks["G"], places=12)
        self.assertAlmostEqual(ranks["G"], ranks["H"], places=12)


# ==========================================================================
# 8. VOLUMEN DE SHUFFLE — respalda los números de ANALYSIS.md
# ==========================================================================

class TestVolumenDeShuffle(unittest.TestCase):
    """
    El mapper debe emitir exactamente N + E pares por iteración. Si este test
    falla, los números del análisis de costo dejan de ser ciertos.
    """

    def test_emite_exactamente_n_mas_e_pares(self):
        graph = pr.load_graph("web_graph_sample.txt")
        n = len(graph)
        e = pr.count_edges(graph)

        _, stats = pr.run_pagerank(graph, max_iter=1, epsilon=0.0)
        self.assertEqual(stats["pairs_per_iteration"][0], n + e)

    def test_el_volumen_es_constante_en_todas_las_iteraciones(self):
        graph = pr.load_graph("web_graph_sample.txt")
        _, stats = pr.run_pagerank(graph)
        self.assertEqual(len(set(stats["pairs_per_iteration"])), 1,
                         "el shuffle debe mover lo mismo en cada iteración")


if __name__ == "__main__":
    unittest.main(verbosity=2)
