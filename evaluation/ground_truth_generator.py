"""
Ground Truth Generator for RAG Pipeline Evaluation
"""

import sqlite3
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime


class GroundTruthGenerator:
    """
    Generates ground truth data for RAG pipeline evaluation
    """
    
    def __init__(self, db_path: str = "data/company.db", schema_path: str = "schema/db_schema.json"):
        self.db_path = db_path
        self.schema_path = schema_path
        self.schema = self._load_schema()
        self.logger = self._setup_logger()
    
    def _load_schema(self) -> Dict:
        """Load database schema"""
        with open(self.schema_path, 'r') as f:
            return json.load(f)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger("ground_truth_generator")
        handler = logging.FileHandler("logs/ground_truth_generation.log")
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
    
    def _execute_query(self, query: str) -> Tuple[List[str], List[tuple]]:
        """Execute a query and return columns and results"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            conn.close()
            return columns, results
        except Exception as e:
            self.logger.error(f"Error executing query '{query}': {str(e)}")
            return [], []
    
    def generate_ground_truth_for_question(self, question: str, expected_sql: str) -> Dict[str, Any]:
        """
        Generate ground truth data for a specific question
        """
        ground_truth = {
            'question': question,
            'expected_sql': expected_sql,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # Execute the expected SQL to get ground truth results
            columns, results = self._execute_query(expected_sql)
            
            ground_truth.update({
                'expected_columns': columns,
                'expected_results': results,
                'expected_result_count': len(results),
                'execution_success': True,
                'execution_error': None
            })
            
            # Generate natural language description of expected results
            ground_truth['expected_description'] = self._generate_result_description(
                question, columns, results
            )
            
        except Exception as e:
            ground_truth.update({
                'expected_columns': [],
                'expected_results': [],
                'expected_result_count': 0,
                'execution_success': False,
                'execution_error': str(e),
                'expected_description': f"Error executing query: {str(e)}"
            })
        
        return ground_truth
    
    def _generate_result_description(self, question: str, columns: List[str], results: List[tuple]) -> str:
        """
        Generate a natural language description of the expected results
        """
        if not results:
            return "No results found for this query."
        
        result_count = len(results)
        
        # Simple heuristics for generating descriptions
        if "count" in question.lower() or "how many" in question.lower():
            if len(results) == 1 and len(results[0]) == 1:
                return f"The count is {results[0][0]}."
            else:
                return f"Found {result_count} records."
        
        elif "top" in question.lower() or "limit" in question.lower():
            return f"Here are the top {min(result_count, 5)} results from the query."
        
        elif "total" in question.lower() or "sum" in question.lower():
            if len(results) == 1 and len(results[0]) == 1:
                return f"The total is {results[0][0]}."
            else:
                return f"Total calculation returned {result_count} records."
        
        elif "list" in question.lower() or "show" in question.lower():
            return f"Here are {result_count} records matching your query."
        
        else:
            return f"Query returned {result_count} records with {len(columns)} columns."
    
    def generate_comprehensive_ground_truth(self, test_cases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate ground truth for a list of test cases
        """
        ground_truths = []
        
        for i, test_case in enumerate(test_cases):
            self.logger.info(f"Generating ground truth for test case {i+1}/{len(test_cases)}")
            
            # Skip if no expected SQL pattern
            if 'expected_sql_pattern' not in test_case or not test_case['expected_sql_pattern']:
                continue
            
            ground_truth = self.generate_ground_truth_for_question(
                test_case['question'],
                test_case['expected_sql_pattern']
            )
            
            # Add test case metadata
            ground_truth.update({
                'test_case_id': test_case.get('test_id', i+1),
                'category': test_case.get('category', 'unknown'),
                'difficulty': test_case.get('difficulty', 'unknown')
            })
            
            ground_truths.append(ground_truth)
        
        return ground_truths
    
    def generate_sql_variations(self, base_sql: str) -> List[str]:
        """
        Generate SQL query variations for robustness testing
        """
        variations = [base_sql]
        
        # Add variations with different formatting
        variations.append(base_sql.upper())
        variations.append(base_sql.lower())
        
        # Add variations with extra spaces
        variations.append(' '.join(base_sql.split()))
        
        # Add variations with different LIMIT values if applicable
        if 'LIMIT' in base_sql.upper():
            variations.append(base_sql.replace('LIMIT 5', 'LIMIT 10'))
            variations.append(base_sql.replace('LIMIT 5', 'LIMIT 3'))
        
        return list(set(variations))  # Remove duplicates
    
    def validate_ground_truth(self, ground_truth_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate the generated ground truth data
        """
        validation_results = {
            'total_cases': len(ground_truth_data),
            'successful_executions': 0,
            'failed_executions': 0,
            'empty_results': 0,
            'non_empty_results': 0,
            'validation_errors': []
        }
        
        for gt in ground_truth_data:
            if gt['execution_success']:
                validation_results['successful_executions'] += 1
                
                if gt['expected_result_count'] == 0:
                    validation_results['empty_results'] += 1
                else:
                    validation_results['non_empty_results'] += 1
            else:
                validation_results['failed_executions'] += 1
                validation_results['validation_errors'].append({
                    'test_case_id': gt.get('test_case_id'),
                    'question': gt['question'],
                    'sql': gt['expected_sql'],
                    'error': gt['execution_error']
                })
        
        return validation_results
    
    def save_ground_truth(self, ground_truth_data: List[Dict[str, Any]], 
                         output_path: str = "evaluation/ground_truth.json") -> None:
        """
        Save ground truth data to file
        """
        # Validation
        validation_results = self.validate_ground_truth(ground_truth_data)
        
        # Prepare data for saving
        output_data = {
            'metadata': {
                'generated_timestamp': datetime.now().isoformat(),
                'total_cases': len(ground_truth_data),
                'validation_summary': validation_results
            },
            'ground_truth_data': ground_truth_data
        }
        
        # Save to JSON
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        self.logger.info(f"Ground truth saved to {output_path}")
        
        # Print summary
        print(f"Ground truth generated for {len(ground_truth_data)} test cases")
        print(f"Successful executions: {validation_results['successful_executions']}")
        print(f"Failed executions: {validation_results['failed_executions']}")
        
        if validation_results['validation_errors']:
            print(f"Validation errors: {len(validation_results['validation_errors'])}")
    
    def load_ground_truth(self, input_path: str = "evaluation/ground_truth.json") -> List[Dict[str, Any]]:
        """
        Load ground truth data from file
        """
        try:
            with open(input_path, 'r') as f:
                data = json.load(f)
            return data.get('ground_truth_data', [])
        except FileNotFoundError:
            self.logger.error(f"Ground truth file not found: {input_path}")
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing ground truth file: {e}")
            return []
    
    def generate_database_profile(self) -> Dict[str, Any]:
        """
        Generate a profile of the database for better ground truth generation
        """
        profile = {
            'timestamp': datetime.now().isoformat(),
            'tables': {}
        }
        
        for table_name in self.schema['tables'].keys():
            try:
                # Get row count
                columns, count_result = self._execute_query(f"SELECT COUNT(*) FROM {table_name}")
                row_count = count_result[0][0] if count_result else 0
                
                # Get sample data
                columns, sample_data = self._execute_query(f"SELECT * FROM {table_name} LIMIT 3")
                
                profile['tables'][table_name] = {
                    'row_count': row_count,
                    'columns': columns,
                    'sample_data': sample_data[:3],  # Limit sample data
                    'column_info': self.schema['tables'][table_name]['columns']
                }
                
            except Exception as e:
                self.logger.error(f"Error profiling table {table_name}: {e}")
                profile['tables'][table_name] = {
                    'error': str(e)
                }
        
        return profile
    
    def enhance_test_cases_with_ground_truth(self, test_cases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enhance test cases with automatically generated ground truth
        """
        enhanced_cases = []
        db_profile = self.generate_database_profile()
        
        for test_case in test_cases:
            enhanced_case = test_case.copy()
            
            # Generate ground truth if expected_sql_pattern exists
            if 'expected_sql_pattern' in test_case and test_case['expected_sql_pattern']:
                ground_truth = self.generate_ground_truth_for_question(
                    test_case['question'],
                    test_case['expected_sql_pattern']
                )
                
                enhanced_case['ground_truth'] = ground_truth
            
            # Add database context for better evaluation
            enhanced_case['db_context'] = {
                'relevant_tables': self._identify_relevant_tables(test_case['question']),
                'estimated_result_size': self._estimate_result_size(test_case['question'], db_profile)
            }
            
            enhanced_cases.append(enhanced_case)
        
        return enhanced_cases
    
    def _identify_relevant_tables(self, question: str) -> List[str]:
        """
        Identify which tables are likely relevant to a question
        """
        question_lower = question.lower()
        relevant_tables = []
        
        # Simple keyword matching
        for table_name in self.schema['tables'].keys():
            if table_name.lower() in question_lower:
                relevant_tables.append(table_name)
        
        # Check for related keywords
        keyword_mappings = {
            'customer': ['customers'],
            'invoice': ['invoices', 'invoice_items'],
            'track': ['tracks'],
            'song': ['tracks'],
            'purchase': ['invoices', 'invoice_items'],
            'buy': ['invoices', 'invoice_items']
        }
        
        for keyword, tables in keyword_mappings.items():
            if keyword in question_lower:
                relevant_tables.extend(tables)
        
        return list(set(relevant_tables))
    
    def _estimate_result_size(self, question: str, db_profile: Dict[str, Any]) -> str:
        """
        Estimate the expected result size based on the question
        """
        question_lower = question.lower()
        
        if 'count' in question_lower or 'how many' in question_lower:
            return 'single_value'
        elif 'top' in question_lower and any(num in question_lower for num in ['5', 'five', '10', 'ten']):
            return 'small_set'
        elif 'all' in question_lower:
            return 'large_set'
        else:
            return 'medium_set'


# Convenience function
def generate_and_save_ground_truth(test_cases_path: str = "evaluation/test_cases.json",
                                  output_path: str = "evaluation/ground_truth.json",
                                  db_path: str = "data/company.db",
                                  schema_path: str = "schema/db_schema.json"):
    """
    Generate and save ground truth data from test cases
    """
    generator = GroundTruthGenerator(db_path, schema_path)
    
    # Load test cases
    try:
        with open(test_cases_path, 'r') as f:
            test_suite = json.load(f)
        test_cases = test_suite.get('test_cases', [])
    except Exception as e:
        print(f"Error loading test cases: {e}")
        return
    
    # Generate ground truth
    ground_truth_data = generator.generate_comprehensive_ground_truth(test_cases)
    
    # Save ground truth
    generator.save_ground_truth(ground_truth_data, output_path)
    
    return ground_truth_data


if __name__ == "__main__":
    # Generate ground truth when run directly
    generate_and_save_ground_truth()