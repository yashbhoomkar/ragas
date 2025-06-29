"""
Test Case Generator for RAG Pipeline Evaluation
"""

import json
import sqlite3
from typing import List, Dict, Any


class TestCaseGenerator:
    """
    Generates comprehensive test cases for RAG pipeline evaluation
    """
    
    def __init__(self, db_path: str = "data/company.db", schema_path: str = "schema/db_schema.json"):
        self.db_path = db_path
        self.schema_path = schema_path
        self.schema = self._load_schema()
    
    def _load_schema(self) -> Dict:
        """Load database schema"""
        with open(self.schema_path, 'r') as f:
            return json.load(f)
    
    def _execute_query(self, query: str) -> List[tuple]:
        """Execute a query and return results"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            conn.close()
            return results
        except Exception as e:
            print(f"Error executing query: {e}")
            return []
    
    def generate_basic_test_cases(self) -> List[Dict[str, Any]]:
        """
        Generate basic test cases covering fundamental operations
        """
        test_cases = [
            # Simple SELECT queries
            {
                "question": "Give me the top 5 customer names",
                "expected_answer": "Here are the top 5 customer names from the database",
                "category": "basic_select",
                "expected_sql_pattern": "SELECT * FROM customers LIMIT 5",
                "difficulty": "easy"
            },
            {
                "question": "Show me all invoice totals",
                "expected_answer": "Here are the invoice totals from the database",
                "category": "basic_select",
                "expected_sql_pattern": "SELECT Total FROM invoices",
                "difficulty": "easy"
            },
            {
                "question": "List all track names",
                "expected_answer": "Here are all the track names from the database",
                "category": "basic_select",
                "expected_sql_pattern": "SELECT Name FROM tracks",
                "difficulty": "easy"
            },
            
            # Grammar correction test cases
            {
                "question": "Give I top a five artist names",
                "expected_answer": "Here are the top 5 artist names",
                "category": "grammar_correction",
                "expected_sql_pattern": "SELECT * LIMIT 5",
                "difficulty": "easy"
            },
            {
                "question": "how many song by artist call Queen",
                "expected_answer": "Information about songs by Queen",
                "category": "grammar_correction",
                "expected_sql_pattern": "SELECT COUNT(*)",
                "difficulty": "medium"
            },
            {
                "question": "list custmer from usa contry",
                "expected_answer": "Here are customers from USA",
                "category": "grammar_correction",
                "expected_sql_pattern": "SELECT * FROM customers WHERE Country",
                "difficulty": "medium"
            },
            
            # Aggregation queries
            {
                "question": "How many customers are there in total?",
                "expected_answer": "The total number of customers in the database",
                "category": "aggregation",
                "expected_sql_pattern": "SELECT COUNT(*) FROM customers",
                "difficulty": "medium"
            },
            {
                "question": "What is the total amount of all invoices?",
                "expected_answer": "The sum of all invoice totals",
                "category": "aggregation",
                "expected_sql_pattern": "SELECT SUM(Total) FROM invoices",
                "difficulty": "medium"
            },
            {
                "question": "Count the number of tracks in the database",
                "expected_answer": "The total count of tracks",
                "category": "aggregation",
                "expected_sql_pattern": "SELECT COUNT(*) FROM tracks",
                "difficulty": "medium"
            },
            
            # Filtering queries
            {
                "question": "Show me customers from Canada",
                "expected_answer": "List of customers from Canada",
                "category": "filtering",
                "expected_sql_pattern": "SELECT * FROM customers WHERE Country = 'Canada'",
                "difficulty": "medium"
            },
            {
                "question": "Find invoices with total greater than 10",
                "expected_answer": "Invoices with total amount greater than 10",
                "category": "filtering",
                "expected_sql_pattern": "SELECT * FROM invoices WHERE Total > 10",
                "difficulty": "medium"
            },
            
            # Sorting queries
            {
                "question": "List customers ordered by last name",
                "expected_answer": "Customers sorted by their last name",
                "category": "sorting",
                "expected_sql_pattern": "SELECT * FROM customers ORDER BY LastName",
                "difficulty": "medium"
            },
            {
                "question": "Show invoices sorted by total amount descending",
                "expected_answer": "Invoices ordered by total amount from highest to lowest",
                "category": "sorting",
                "expected_sql_pattern": "SELECT * FROM invoices ORDER BY Total DESC",
                "difficulty": "medium"
            },
            
            # Complex queries (potential JOIN scenarios)
            {
                "question": "Which customers have made purchases?",
                "expected_answer": "Customers who have associated invoices",
                "category": "complex",
                "expected_sql_pattern": "SELECT DISTINCT customers",
                "difficulty": "hard"
            },
            
            # Edge cases
            {
                "question": "Show me nothing",
                "expected_answer": "No specific data requested",
                "category": "edge_case",
                "expected_sql_pattern": "",
                "difficulty": "hard"
            },
            {
                "question": "What is the meaning of life?",
                "expected_answer": "This question is not related to the database",
                "category": "edge_case",
                "expected_sql_pattern": "",
                "difficulty": "hard"
            },
            {
                "question": "",
                "expected_answer": "Please provide a valid question",
                "category": "edge_case",
                "expected_sql_pattern": "",
                "difficulty": "hard"
            }
        ]
        
        return test_cases
    
    def generate_schema_based_test_cases(self) -> List[Dict[str, Any]]:
        """
        Generate test cases based on the actual database schema
        """
        test_cases = []
        
        for table_name, table_info in self.schema['tables'].items():
            # Basic table queries
            test_cases.append({
                "question": f"Show me all data from {table_name}",
                "expected_answer": f"All records from the {table_name} table",
                "category": "schema_based",
                "expected_sql_pattern": f"SELECT * FROM {table_name}",
                "difficulty": "easy"
            })
            
            # Column-specific queries
            for column_name, column_desc in table_info['columns'].items():
                test_cases.append({
                    "question": f"List all {column_name} from {table_name}",
                    "expected_answer": f"All {column_name} values from {table_name}",
                    "category": "schema_based",
                    "expected_sql_pattern": f"SELECT {column_name} FROM {table_name}",
                    "difficulty": "easy"
                })
        
        return test_cases
    
    def generate_data_driven_test_cases(self) -> List[Dict[str, Any]]:
        """
        Generate test cases based on actual data in the database
        """
        test_cases = []
        
        # Get sample data to create realistic test cases
        try:
            # Check what countries exist in customers table
            countries = self._execute_query("SELECT DISTINCT Country FROM customers LIMIT 5")
            for country_tuple in countries:
                country = country_tuple[0]
                test_cases.append({
                    "question": f"How many customers are from {country}?",
                    "expected_answer": f"The number of customers from {country}",
                    "category": "data_driven",
                    "expected_sql_pattern": f"SELECT COUNT(*) FROM customers WHERE Country = '{country}'",
                    "difficulty": "medium"
                })
            
            # Check invoice date ranges
            date_range = self._execute_query("SELECT MIN(InvoiceDate), MAX(InvoiceDate) FROM invoices")
            if date_range:
                test_cases.append({
                    "question": "What is the date range of invoices?",
                    "expected_answer": "The earliest and latest invoice dates",
                    "category": "data_driven",
                    "expected_sql_pattern": "SELECT MIN(InvoiceDate), MAX(InvoiceDate) FROM invoices",
                    "difficulty": "medium"
                })
            
            # Check top invoice amounts
            test_cases.append({
                "question": "What are the top 3 highest invoice amounts?",
                "expected_answer": "The three highest invoice totals",
                "category": "data_driven",
                "expected_sql_pattern": "SELECT Total FROM invoices ORDER BY Total DESC LIMIT 3",
                "difficulty": "medium"
            })
            
        except Exception as e:
            print(f"Error generating data-driven test cases: {e}")
        
        return test_cases
    
    def generate_error_test_cases(self) -> List[Dict[str, Any]]:
        """
        Generate test cases that should trigger errors or edge cases
        """
        return [
            {
                "question": "Select from nonexistent_table",
                "expected_answer": "Error: Table does not exist",
                "category": "error_case",
                "expected_sql_pattern": "",
                "difficulty": "hard",
                "should_error": True
            },
            {
                "question": "Show me data from xyz column",
                "expected_answer": "Error: Column does not exist",
                "category": "error_case",
                "expected_sql_pattern": "",
                "difficulty": "hard",
                "should_error": True
            },
            {
                "question": "DELETE all customers",
                "expected_answer": "This operation is not supported",
                "category": "error_case",
                "expected_sql_pattern": "",
                "difficulty": "hard",
                "should_error": True
            }
        ]
    
    def generate_comprehensive_test_suite(self) -> List[Dict[str, Any]]:
        """
        Generate a comprehensive test suite combining all test case types
        """
        all_test_cases = []
        
        # Basic test cases
        all_test_cases.extend(self.generate_basic_test_cases())
        
        # Schema-based test cases (limited to avoid too many)
        schema_cases = self.generate_schema_based_test_cases()[:10]  # Limit to 10
        all_test_cases.extend(schema_cases)
        
        # Data-driven test cases
        all_test_cases.extend(self.generate_data_driven_test_cases())
        
        # Error test cases
        all_test_cases.extend(self.generate_error_test_cases())
        
        # Add metadata
        for i, test_case in enumerate(all_test_cases):
            test_case['test_id'] = i + 1
            test_case['generated_timestamp'] = None  # Will be set when saved
        
        return all_test_cases
    
    def save_test_cases(self, test_cases: List[Dict[str, Any]], output_path: str = "evaluation/test_cases.json"):
        """
        Save test cases to JSON file
        """
        from datetime import datetime
        
        # Add timestamp to each test case
        for test_case in test_cases:
            test_case['generated_timestamp'] = datetime.now().isoformat()
        
        # Create test suite metadata
        test_suite = {
            "metadata": {
                "total_cases": len(test_cases),
                "generated_timestamp": datetime.now().isoformat(),
                "categories": list(set(tc['category'] for tc in test_cases)),
                "difficulty_levels": list(set(tc['difficulty'] for tc in test_cases))
            },
            "test_cases": test_cases
        }
        
        with open(output_path, 'w') as f:
            json.dump(test_suite, f, indent=2)
        
        print(f"Test cases saved to {output_path}")
        print(f"Total test cases: {len(test_cases)}")
        
        # Print summary
        categories = {}
        difficulties = {}
        for tc in test_cases:
            categories[tc['category']] = categories.get(tc['category'], 0) + 1
            difficulties[tc['difficulty']] = difficulties.get(tc['difficulty'], 0) + 1
        
        print("\nTest Case Summary:")
        print("Categories:", categories)
        print("Difficulties:", difficulties)
        
        return test_suite
    
    def load_test_cases(self, input_path: str = "evaluation/test_cases.json") -> List[Dict[str, Any]]:
        """
        Load test cases from JSON file
        """
        try:
            with open(input_path, 'r') as f:
                test_suite = json.load(f)
            return test_suite.get('test_cases', [])
        except FileNotFoundError:
            print(f"Test cases file not found: {input_path}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error parsing test cases file: {e}")
            return []


# Convenience function to generate and save test cases
def generate_and_save_test_cases(db_path: str = "data/company.db", 
                                 schema_path: str = "schema/db_schema.json",
                                 output_path: str = "evaluation/test_cases.json"):
    """
    Generate and save a comprehensive test suite
    """
    generator = TestCaseGenerator(db_path, schema_path)
    test_cases = generator.generate_comprehensive_test_suite()
    return generator.save_test_cases(test_cases, output_path)


if __name__ == "__main__":
    # Generate test cases when run directly
    generate_and_save_test_cases()