import json
import ollama
import sqlite3

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

def generate_sql(schema_text, user_question, model="llama3"):
    prompt = f"""
You are a helpful assistant. Given the following database schema and the user's question, generate a correct SQL query compatible with SQLite. Return only the SQL query and nothing else.

Schema:
{schema_text}

User Question:
{user_question}
"""
    response = ollama.chat(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )
    return response['message']['content'].strip()


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
    
if __name__ == "__main__":
    schema_text = load_and_format_schema()
    user_question = input("❓ Ask a question about the database: ")

    print("\n🤖 Generating SQL query using Ollama...")
    sql_query = generate_sql(schema_text, user_question)
    print(f"\n📝 SQL Query:\n{sql_query}")

    print("\n📦 Executing SQL query on company.db...")
    columns, results = execute_sql(sql_query)

    if isinstance(results, str):
        print(results)  # Error message
    else:
        print("\n✅ Query Results:\n")
        print(columns)
        for row in results:
            print(row)
