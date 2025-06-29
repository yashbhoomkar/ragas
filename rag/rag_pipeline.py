# rag/rag_pipeline.py

import sqlite3
from rag.utils import load_and_format_schema, init_logger, log_interaction
from rag.query_generator import generate_sql
from rag.result_interpreter import interpret_result
from rag.prompt_enhancer import enhance_question 

def execute_sql(query, db_path="data/company.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        conn.close()
        return columns, results
    except Exception as e:
        conn.close()
        return None, f"❌ Error executing query: {str(e)}"

def main():
    init_logger()
    schema_text = load_and_format_schema()
    raw_question = input("❓ Ask a question about the database: ")

    print("\n🧹 Enhancing grammar with prompt_enhancer (Mistral)...")
    enhanced_question = enhance_question(raw_question)

    print("\n🤖 Generating SQL query using Ollama (LLaMA 3)...")
    sql_query = generate_sql(schema_text, enhanced_question)
    print(f"\n📝 SQL Query:\n{sql_query}")

    print("\n📦 Executing SQL query on company.db...")
    columns, results = execute_sql(sql_query)

    if isinstance(results, str):
        print(results)
        return

    print("\n✅ Query Results:")
    print(columns)
    for row in results:
        print(row)

    print("\n🧠 Passing result to LLM for explanation...")
    explanation = interpret_result(enhanced_question, sql_query, results, columns)
    print(f"\n💬 Explanation:\n{explanation}")

    log_interaction(raw_question, sql_query, results, explanation)

if __name__ == "__main__":
    main()
