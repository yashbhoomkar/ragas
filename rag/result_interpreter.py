import ollama

def interpret_result(user_question, sql_query, data, columns, model="llama3", max_rows=10):
    # Only show first N rows if too many
    if len(data) > max_rows:
        data = data[:max_rows]

    result_text = "\n".join(
        [", ".join(f"{columns[i]}: {row[i]}" for i in range(len(columns))) for row in data]
    )

    prompt = f"""
You are a powerful and helpful AI assistant integrated into a Retrieval-Augmented Generation (RAG) system. Your role is to interpret the results of SQL queries executed on a SQLite3 database and respond clearly to the user.

---

Your responsibilities:

1. Understand the user’s original question.
2. Understand the SQL query that was generated and executed.
3. Review the data retrieved from the database.
4. Decide how to respond based on the user's intent.

---

Interpretation Logic:

- If the user explicitly asks for the data **"as it is"**, or says things like **"just show the data"**, **"only return the raw results"**, or similar, then:
    - Return the data only.
    - Do NOT provide any explanation, context, summary, or interpretation.
    - Format the response in a clean and readable tabular structure.
    - Preserve data types and accuracy — no rounding or paraphrasing.

- If the user asks a general, analytical, or interpretive question (e.g., "Who are the top customers?", "What countries have most users?"), then:
    - Explain the result clearly and concisely.
    - Use simple language as if talking to a beginner.
    - Mention what the query did.
    - Highlight interesting points if visible (like patterns, counts, top entries, etc.).
    - Keep it short and human-readable.

---

Inputs:

- User Question: "{user_question}"
- Executed SQL Query:
{sql_query}

- Query Result Preview (first {len(data)} rows):
{result_text}

---

Based on the user's request, decide the appropriate response type.

Only return your final response. Do NOT repeat these instructions.
"""

    response = ollama.chat(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )
    return response['message']['content'].strip()
