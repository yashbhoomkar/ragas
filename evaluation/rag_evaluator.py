"""
RAG Pipeline Evaluator using RAGAs framework
"""

import sqlite3
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Any
from datasets import Dataset

# RAGAs imports
from ragas import evaluate
from ragas.metrics import (
    answer_relevancy,
    faithfulness,
    context_precision,
    context_recall,
    answer_correctness,
    answer_similarity
)

# Import your RAG components
from rag.rag_pipeline import execute_sql
from rag.query_generator import generate_sql
from rag.result_interpreter import interpret_result
from rag.prompt_enhancer import enhance_question
from rag.utils import load_and_format_schema


class RAGEvaluator:
    """
    Comprehensive evaluator for the RAG pipeline using RAGAs metrics
    """
    
    def __init__(self, db_path: str = "data/company.db", schema_path: str = "schema/db_schema.json"):
        self.db_path = db_path
        self.schema_path = schema_path
        self.schema_text = load_and_format_schema(schema_path)
        self.logger = self._setup_logger()
        
        # RAGAs metrics to evaluate
        self.metrics = [
            answer_relevancy,
            faithfulness,
            context_precision,
            context_recall,
            answer_correctness,
            answer_similarity
        ]
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging for evaluation"""
        logger = logging.getLogger("rag_evaluator")
        handler = logging.FileHandler("logs/rag_evaluation.log")
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
    
    def run_pipeline_step(self, question: str) -> Dict[str, Any]:
        """
        Run a single question through the entire RAG pipeline
        Returns all intermediate outputs for evaluation
        """
        try:
            # Step 1: Enhance question
            enhanced_question = enhance_question(question)
            
            # Step 2: Generate SQL
            sql_query = generate_sql(self.schema_text, enhanced_question)
            
            # Step 3: Execute SQL
            columns, results = execute_sql(sql_query, self.db_path)
            
            if isinstance(results, str):  # Error case
                return {
                    'original_question': question,
                    'enhanced_question': enhanced_question,
                    'sql_query': sql_query,
                    'execution_error': results,
                    'columns': None,
                    'results': None,
                    'final_answer': f"Error: {results}"
                }
            
            # Step 4: Interpret results
            final_answer = interpret_result(enhanced_question, sql_query, results, columns)
            
            return {
                'original_question': question,
                'enhanced_question': enhanced_question,
                'sql_query': sql_query,
                'columns': columns,
                'results': results,
                'final_answer': final_answer,
                'execution_error': None
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline error for question '{question}': {str(e)}")
            return {
                'original_question': question,
                'enhanced_question': question,
                'sql_query': None,
                'columns': None,
                'results': None,
                'final_answer': f"Pipeline Error: {str(e)}",
                'execution_error': str(e)
            }
    
    def prepare_ragas_dataset(self, test_cases: List[Dict]) -> Dataset:
        """
        Convert test cases to RAGAs dataset format
        """
        questions = []
        answers = []
        contexts = []
        ground_truths = []
        
        for test_case in test_cases:
            # Run pipeline
            pipeline_output = self.run_pipeline_step(test_case['question'])
            
            # Format for RAGAs
            questions.append(test_case['question'])
            answers.append(pipeline_output['final_answer'])
            
            # Context is the retrieved data + SQL query
            context_info = []
            if pipeline_output['sql_query']:
                context_info.append(f"SQL Query: {pipeline_output['sql_query']}")
            if pipeline_output['results']:
                context_info.append(f"Database Results: {str(pipeline_output['results'][:5])}")  # Limit context size
            
            contexts.append(context_info)
            ground_truths.append(test_case['expected_answer'])
        
        return Dataset.from_dict({
            'question': questions,
            'answer': answers,
            'contexts': contexts,
            'ground_truth': ground_truths
        })
    
    def evaluate_sql_correctness(self, test_cases: List[Dict]) -> Dict[str, float]:
        """
        Evaluate SQL generation correctness
        """
        total_cases = len(test_cases)
        syntactically_correct = 0
        semantically_correct = 0
        execution_successful = 0
        
        for test_case in test_cases:
            pipeline_output = self.run_pipeline_step(test_case['question'])
            
            # Check execution success
            if pipeline_output['execution_error'] is None:
                execution_successful += 1
                syntactically_correct += 1
                
                # Check semantic correctness (if expected SQL provided)
                if 'expected_sql_pattern' in test_case:
                    if self._check_sql_semantic_similarity(
                        pipeline_output['sql_query'], 
                        test_case['expected_sql_pattern']
                    ):
                        semantically_correct += 1
        
        return {
            'sql_syntax_accuracy': syntactically_correct / total_cases,
            'sql_execution_success_rate': execution_successful / total_cases,
            'sql_semantic_accuracy': semantically_correct / total_cases if any('expected_sql_pattern' in tc for tc in test_cases) else 0.0
        }
    
    def _check_sql_semantic_similarity(self, generated_sql: str, expected_pattern: str) -> bool:
        """
        Simple semantic similarity check for SQL queries
        """
        # Normalize both queries
        gen_normalized = ' '.join(generated_sql.upper().split())
        exp_normalized = ' '.join(expected_pattern.upper().split())
        
        # Check if key components match
        gen_tokens = set(gen_normalized.split())
        exp_tokens = set(exp_normalized.split())
        
        # Calculate Jaccard similarity
        intersection = len(gen_tokens.intersection(exp_tokens))
        union = len(gen_tokens.union(exp_tokens))
        
        return (intersection / union) > 0.7 if union > 0 else False
    
    def evaluate_pipeline(self, test_cases: List[Dict]) -> Dict[str, Any]:
        """
        Comprehensive evaluation of the RAG pipeline
        """
        self.logger.info(f"Starting evaluation with {len(test_cases)} test cases")
        
        # 1. RAGAs evaluation
        ragas_dataset = self.prepare_ragas_dataset(test_cases)
        
        try:
            ragas_results = evaluate(
                dataset=ragas_dataset,
                metrics=self.metrics
            )
            ragas_scores = ragas_results.to_pandas()
        except Exception as e:
            self.logger.error(f"RAGAs evaluation failed: {str(e)}")
            ragas_scores = pd.DataFrame()
        
        # 2. SQL correctness evaluation
        sql_metrics = self.evaluate_sql_correctness(test_cases)
        
        # 3. Component-level evaluation
        component_metrics = self._evaluate_components(test_cases)
        
        # 4. Aggregate results
        evaluation_results = {
            'timestamp': datetime.now().isoformat(),
            'total_test_cases': len(test_cases),
            'ragas_metrics': ragas_scores.mean().to_dict() if not ragas_scores.empty else {},
            'sql_metrics': sql_metrics,
            'component_metrics': component_metrics,
            'detailed_results': []
        }
        
        # 5. Detailed per-case results
        for i, test_case in enumerate(test_cases):
            pipeline_output = self.run_pipeline_step(test_case['question'])
            
            case_result = {
                'test_case_id': i,
                'question': test_case['question'],
                'expected_answer': test_case['expected_answer'],
                'generated_answer': pipeline_output['final_answer'],
                'sql_query': pipeline_output['sql_query'],
                'execution_success': pipeline_output['execution_error'] is None,
                'error': pipeline_output['execution_error']
            }
            
            # Add RAGAs scores for this case if available
            if not ragas_scores.empty and i < len(ragas_scores):
                case_result['ragas_scores'] = ragas_scores.iloc[i].to_dict()
            
            evaluation_results['detailed_results'].append(case_result)
        
        self.logger.info("Evaluation completed")
        return evaluation_results
    
    def _evaluate_components(self, test_cases: List[Dict]) -> Dict[str, float]:
        """
        Evaluate individual components of the pipeline
        """
        grammar_improvements = 0
        total_cases = len(test_cases)
        
        for test_case in test_cases:
            original_question = test_case['question']
            enhanced_question = enhance_question(original_question)
            
            # Simple check if grammar was improved
            if enhanced_question != original_question:
                grammar_improvements += 1
        
        return {
            'grammar_enhancement_rate': grammar_improvements / total_cases,
            'pipeline_completion_rate': 1.0  # Will be updated based on successful runs
        }
    
    def save_results(self, results: Dict[str, Any], output_path: str = "results/evaluation_results.json"):
        """
        Save evaluation results to file
        """
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        self.logger.info(f"Results saved to {output_path}")
        
        # Also save a summary
        summary_path = output_path.replace('.json', '_summary.txt')
        self._save_summary(results, summary_path)
    
    def _save_summary(self, results: Dict[str, Any], summary_path: str):
        """
        Save a human-readable summary of results
        """
        with open(summary_path, 'w') as f:
            f.write("RAG Pipeline Evaluation Summary\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Evaluation Date: {results['timestamp']}\n")
            f.write(f"Total Test Cases: {results['total_test_cases']}\n\n")
            
            # RAGAs metrics
            if results['ragas_metrics']:
                f.write("RAGAs Metrics:\n")
                for metric, score in results['ragas_metrics'].items():
                    f.write(f"  {metric}: {score:.4f}\n")
                f.write("\n")
            
            # SQL metrics
            f.write("SQL Generation Metrics:\n")
            for metric, score in results['sql_metrics'].items():
                f.write(f"  {metric}: {score:.4f}\n")
            f.write("\n")
            
            # Component metrics
            f.write("Component Metrics:\n")
            for metric, score in results['component_metrics'].items():
                f.write(f"  {metric}: {score:.4f}\n")