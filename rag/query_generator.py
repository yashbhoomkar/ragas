# rag/query_generator.py

import ollama

def generate_sql(schema_text, enhanced_question, model="llama3"):
    prompt = f"""
You are an advanced AI system that specializes in translating user questions into precise and executable SQL queries for a SQLite3 database.

---

Role:
You act as a database query generator in a Retrieval-Augmented Generation (RAG) pipeline. Your task is to take a user's natural language question and output a syntactically correct and efficient SQL query that runs on a SQLite database.

---

Inputs:
1. A description of the database schema (tables and columns)
2. A user question in natural language

---

Instructions:

- Generate a valid SQL query that directly answers the user's question using only the information provided in the schema.
- Use proper SQLite syntax. Ensure the query is syntactically correct.
- Do NOT include explanations, comments, summaries, or anything except the raw SQL query as your output.

---

Query Guidelines:

- Reference only real tables and columns present in the schema. Never guess or fabricate schema elements.
- If the user asks to retrieve data (e.g., "list", "show", "get"), and does not explicitly ask for all results, include a `LIMIT 5` clause.
- Avoid using `SELECT *`. Select only the relevant columns unless the user asks for all columns.
- Respect filters, conditions, and ordering implied by the user question.
- Support typical SQL clauses: WHERE, ORDER BY, GROUP BY, LIMIT, etc., as needed.
- If the question is ambiguous, choose the safest interpretation that avoids unnecessary joins or assumptions.

---

Schema:
{schema_text}

User Question:
{enhanced_question}

---

Output:
Only return the generated SQL query, nothing else.
"""
    #print("debugging print--------------------" , enhanced_question)
    response = ollama.chat(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )
    return response['message']['content'].strip()
