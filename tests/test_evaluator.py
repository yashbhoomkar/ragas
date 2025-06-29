# tests/test_evaluator.py

import unittest
from evaluation.rag_evaluator import RAGEvaluator

class TestRAGEvaluator(unittest.TestCase):
    def setUp(self):
        self.evaluator = RAGEvaluator()

    def test_ragas_evaluation_runs(self):
        try:
            self.evaluator.run_evaluation()
        except Exception as e:
            self.fail(f"RAG evaluator crashed: {e}")

    def test_sql_accuracy_metric(self):
        metrics = self.evaluator.run_evaluation()
        self.assertIn("sql_accuracy", metrics, "sql_accuracy not found in metrics")
        self.assertIsInstance(metrics["sql_accuracy"], float, "sql_accuracy should be a float")

    def test_interpretation_quality_metric(self):
        metrics = self.evaluator.run_evaluation()
        self.assertIn("interpretation_quality", metrics, "interpretation_quality not found in metrics")
        self.assertIsInstance(metrics["interpretation_quality"], float, "interpretation_quality should be a float")

if __name__ == "__main__":
    unittest.main()
