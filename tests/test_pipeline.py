"""
Unit tests for RAG Pipeline components
"""

import unittest
import sqlite3
import tempfile
import json
import os
from unittest.mock import patch, MagicMock

# Import RAG components
from rag.rag_pipeline import execute_sql
from rag.query_generator import generate_sql
from rag.result_interpreter import interpret_result
from rag.prompt_enhancer import enhance_question
from rag.utils import load_and_format_schema, log_interaction


class TestRAGPipeline(unittest.TestCase):
    """
    Test cases for the RAG pipeline components
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up test database and schema"""
        # Create temporary database
        cls.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        cls.test_db_path = cls.test_db.name
        cls.test_db.close()
        
        # Create test schema file
        cls.test_schema = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        cls.test_schema_path = cls.test_schema.name
        
        # Sample schema for testing
        test_schema_data = {
            "tables": {
                "test_customers": {
                    "columns": {
                        "id": "Customer ID",
                        "name": "Customer name",
                        "email": "Customer email"
                    }
                },
                "test_orders": {
                    "columns": {
                        "order_id": "Order ID",
                        "customer_id": "Customer ID",
                        "total": "Order total"
                    }
                }
            }
        }
        
        json.dump(test_schema_data, cls.test_schema)
        cls.test_schema.close()
        
        # Initialize test database
        cls._create_test_database()
    
    @classmethod
    def _create_test_database(cls):
        """Create test database with sample data"""
        conn = sqlite3.connect(cls.test_db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE test_customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE test_orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                total REAL
            )
        ''')
        
        # Insert sample data
        cursor.executemany('''
            INSERT INTO test_customers (id, name, email) VALUES (?, ?, ?)
        ''', [
            (1, 'John Doe', 'john@example.com'),
            (2, 'Jane Smith', 'jane@example.com'),
            (3, 'Bob Johnson', 'bob@example.com')
        ])
        
        cursor.executemany('''
            INSERT INTO test_orders (order_id, customer_id, total) VALUES (?, ?, ?)
        ''', [
            (1, 1, 100.50),
            (2, 2, 75.25),
            (3, 1, 200.00)
        ])
        
        conn.commit()
        conn.close()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test files"""
        try:
            os.unlink(cls.test_db_path)
            os.unlink(cls.test_schema_path)
        except:
            pass
    
    def test_execute_sql_success(self):
        """Test successful SQL execution"""
        query = "SELECT * FROM test_customers"
        columns, results = execute_sql(query, self.test_db_path)
        
        self.assertIsNotNone(columns)
        self.assertIsNotNone(results)
        self.assertEqual(len(columns), 3)  # id, name, email
        self.assertEqual(len(results), 3)  # 3 customers
        self.assertIn('id', columns)
        self.assertIn('name', columns)
        self.assertIn('email', columns)
    
    def test_execute_sql_error(self):
        """Test SQL execution with error"""
        query = "SELECT * FROM nonexistent_table"
        columns, results = execute_sql(query, self.test_db_path)
        
        self.assertIsNone(columns)
        self.assertIsInstance(results, str)
        self.assertIn("Error executing query", results)
    
    def test_execute_sql_empty_result(self):
        """Test SQL execution with empty result"""
        query = "SELECT * FROM test_customers WHERE id = 999"
        columns, results = execute_sql(query, self.test_db_path)
        
        self.assertIsNotNone(columns)
        self.assertEqual(len(results), 0)
    
    def test_load_and_format_schema(self):
        """Test schema loading and formatting"""
        formatted_schema = load_and_format_schema(self.test_schema_path)
        
        self.assertIsInstance(formatted_schema, str)
        self.assertIn("test_customers", formatted_schema)
        self.assertIn("test_orders", formatted_schema)
        self.assertIn("Customer ID", formatted_schema)
        self.assertIn("Order total", formatted_schema)
    
    @patch('rag.prompt_enhancer.ollama.chat')
    def test_enhance_question_success(self, mock_ollama):
        """Test successful question enhancement"""
        # Mock Ollama response
        mock_ollama.return_value = {
            'message': {'content': 'Give me the top 5 customer names'}
        }
        
        original_question = "Give I top a five customer names"
        enhanced = enhance_question(original_question)
        
        self.assertEqual(enhanced, 'Give me the top 5 customer names')
        mock_ollama.assert_called_once()
    
    @patch('rag.prompt_enhancer.ollama.chat')
    def test_enhance_question_error(self, mock_ollama):
        """Test question enhancement with error"""
        # Mock Ollama error
        mock_ollama.side_effect = Exception("Ollama error")
        
        original_question = "Give I top a five customer names"
        enhanced = enhance_question(original_question)
        
        # Should return original question on error
        self.assertEqual(enhanced, original_question)
    
    @patch('rag.query_generator.ollama.chat')
    def test_generate_sql_success(self, mock_ollama):
        """Test successful SQL generation"""
        # Mock Ollama response
        mock_ollama.return_value = {
            'message': {'content': 'SELECT name FROM test_customers LIMIT 5'}
        }
        
        schema_text = "Table: test_customers\n  - name: Customer name"
        question = "Give me the top 5 customer names"
        
        sql = generate_sql(schema_text, question)
        
        self.assertEqual(sql, 'SELECT name FROM test_customers LIMIT 5')
        mock_ollama.assert_called_once()
    
    @patch('rag.result_interpreter.ollama.chat')
    def test_interpret_result_success(self, mock_ollama):
        """Test successful result interpretation"""
        # Mock Ollama response
        mock_ollama.return_value = {
            'message': {'content': 'Here are the customer names: John Doe, Jane Smith, Bob Johnson'}
        }
        
        question = "Give me customer names"
        sql_query = "SELECT name FROM test_customers"
        data = [('John Doe',), ('Jane Smith',), ('Bob Johnson',)]
        columns = ['name']
        
        interpretation = interpret_result(question, sql_query, data, columns)
        
        self.assertIn('John Doe', interpretation)
        self.assertIn('Jane Smith', interpretation)
        self.assertIn('Bob Johnson', interpretation)
        mock_ollama.assert_called_once()
    
    def test_interpret_result_large_dataset(self):
        """Test result interpretation with large dataset (should be truncated)"""
        with patch('rag.result_interpreter.ollama.chat') as mock_ollama:
            mock_ollama.return_value = {
                'message': {'content': 'Here are the first 10 results...'}
            }
            
            # Create large dataset
            large_data = [(f'Customer {i}',) for i in range(50)]
            columns = ['name']
            
            interpretation = interpret_result(
                "List all customers", 
                "SELECT name FROM customers", 
                large_data, 
                columns,
                max_rows=10
            )
            
            self.assertIsInstance(interpretation, str)
            mock_ollama.assert_called_once()
    
    @patch('logging.info')
    def test_log_interaction(self, mock_logging):
        """Test interaction logging"""
        user_query = "Test query"
        sql_query = "SELECT * FROM test"
        results = [('result1',), ('result2',)]
        explanation = "Test explanation"
        
        log_interaction(user_query, sql_query, results, explanation)
        
        # Check if logging was called
        self.assertTrue(mock_logging.called)
        self.assertEqual(mock_logging.call_count, 4)  # 4 log calls in log_interaction


class TestRAGPipelineIntegration(unittest.TestCase):
    """
    Integration tests for the complete RAG pipeline
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up for integration tests"""
        # Use the same test setup as unit tests
        TestRAGPipeline.setUpClass()
        cls.test_db_path = TestRAGPipeline.test_db_path
        cls.test_schema_path = TestRAGPipeline.test_schema_path
    
    @classmethod
    def tearDownClass(cls):
        """Clean up integration test files"""
        TestRAGPipeline.tearDownClass()
    
    @patch('rag.prompt_enhancer.ollama.chat')
    @patch('rag.query_generator.ollama.chat')
    @patch('rag.result_interpreter.ollama.chat')
    def test_full_pipeline_flow(self, mock_interpreter, mock_generator, mock_enhancer):
        """Test the complete pipeline flow"""
        # Mock all Ollama calls
        mock_enhancer.return_value = {
            'message': {'content': 'Give me all customer names'}
        }
        mock_generator.return_value = {
            'message': {'content': 'SELECT name FROM test_customers'}
        }
        mock_interpreter.return_value = {
            'message': {'content': 'Here are all customer names: John Doe, Jane Smith, Bob Johnson'}
        }
        
        # Test the flow
        original_question = "Give I all customer name"
        
        # Step 1: Enhance question
        enhanced_question = enhance_question(original_question)
        self.assertEqual(enhanced_question, 'Give me all customer names')
        
        # Step 2: Generate SQL
        schema_text = load_and_format_schema(self.test_schema_path)
        sql_query = generate_sql(schema_text, enhanced_question)
        self.assertEqual(sql_query, 'SELECT name FROM test_customers')
        
        # Step 3: Execute SQL
        columns, results = execute_sql(sql_query, self.test_db_path)
        self.assertIsNotNone(columns)
        self.assertIsNotNone(results)
        self.assertEqual(len(results), 3)
        
        # Step 4: Interpret results
        interpretation = interpret_result(enhanced_question, sql_query, results, columns)
        self.assertIn('John Doe', interpretation)
        
        # Verify all mocks were called
        mock_enhancer.assert_called_once()
        mock_generator.assert_called_once()
        mock_interpreter.assert_called_once()
    
    def test_pipeline_with_invalid_sql(self):
        """Test pipeline behavior with invalid SQL"""
        with patch('rag.query_generator.ollama.chat') as mock_generator:
            mock_generator.return_value = {
                'message': {'content': 'INVALID SQL QUERY'}
            }
            
            # This should result in an execution error
            columns, results = execute_sql('INVALID SQL QUERY', self.test_db_path)
            
            self.assertIsNone(columns)
            self.assertIsInstance(results, str)
            self.assertIn("Error executing query", results)
    
    def test_pipeline_with_empty_results(self):
        """Test pipeline behavior with queries returning no results"""
        with patch('rag.result_interpreter.ollama.chat') as mock_interpreter:
            mock_interpreter.return_value = {
                'message': {'content': 'No customers found matching the criteria.'}
            }
            
            # Query that returns no results
            columns, results = execute_sql(
                "SELECT name FROM test_customers WHERE id = 999", 
                self.test_db_path
            )
            
            self.assertIsNotNone(columns)
            self.assertEqual(len(results), 0)
            
            # Interpret empty results
            interpretation = interpret_result(
                "Find customers with ID 999", 
                "SELECT name FROM test_customers WHERE id = 999",
                results, 
                columns
            )
            
            self.assertIn('No customers found', interpretation)


class TestRAGPipelineErrorHandling(unittest.TestCase):
    """
    Test error handling in RAG pipeline
    """
    
    def test_execute_sql_with_invalid_db_path(self):
        """Test SQL execution with invalid database path"""
        columns, results = execute_sql("SELECT 1", "/nonexistent/path.db")
        
        self.assertIsNone(columns)
        self.assertIsInstance(results, str)
        self.assertIn("Error executing query", results)
    
    def test_load_schema_with_invalid_path(self):
        """Test schema loading with invalid path"""
        with self.assertRaises(FileNotFoundError):
            load_and_format_schema("/nonexistent/schema.json")
    
    def test_load_schema_with_invalid_json(self):
        """Test schema loading with invalid JSON"""
        # Create invalid JSON file
        invalid_json = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        invalid_json.write("{ invalid json")
        invalid_json.close()
        
        try:
            with self.assertRaises(json.JSONDecodeError):
                load_and_format_schema(invalid_json.name)
        finally:
            os.unlink(invalid_json.name)


if __name__ == '__main__':
    unittest.main()