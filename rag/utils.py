import json
import logging
from datetime import datetime

def load_and_format_schema(schema_path="schema/db_schema.json"):
    with open(schema_path, "r") as f:
        schema = json.load(f)
    
    formatted = ""
    for table, content in schema["tables"].items():
        formatted += f"Table: {table}\n"
        for column, desc in content["columns"].items():
            formatted += f"  - {column}: {desc}\n"
        formatted += "\n"
    
    return formatted

def init_logger(log_path="logs/rag.log"):
    logging.basicConfig(
        filename=log_path,
        filemode='a',
        format='%(asctime)s - %(message)s',
        level=logging.INFO
    )

def log_interaction(user_query, sql_query, results, explanation):
    logging.info("USER QUESTION: %s", user_query)
    logging.info("SQL GENERATED: %s", sql_query)
    logging.info("DB RESULT: %s", results)
    logging.info("LLM EXPLANATION: %s\n", explanation)
