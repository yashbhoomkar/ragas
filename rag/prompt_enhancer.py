import ollama
import logging

logger = logging.getLogger(__name__)

def enhance_question(user_question: str, model: str = "mistral") -> str:
    prompt = f"""
You are an expert **grammar correction assistant** in a Retrieval-Augmented Generation (RAG) system.

---

Objective:
Improve the grammar, spelling, and phrasing of the user’s question **without altering the intent or meaning**. Your output will be passed to another language model that generates SQL queries, so precision is essential.

---

Instructions:
- Correct **only grammar and phrasing** issues.
- Do **not** rephrase or restructure unless absolutely necessary.
- Retain all original nouns, keywords, and intent as-is.
- Do **not** explain, comment, or return anything other than the corrected question.
- If the question is already grammatically correct, return it unchanged.

---

Examples:

 Input: "Give me name and email from cutomers tabel"
 Output: "Give me the name and email from the customers table"

 Input: "list top 5 row in databse"
 Output: "List the top 5 rows in the database"

 Input: "how many song by artist call 'Queen'"
 Output: "How many songs are by the artist called 'Queen'?"

---

Input:
{user_question}

 Output (corrected question):
"""

    try:
        response = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}]
        )
        enhanced = response['message']['content'].strip()
        logger.info(f"Original Question: {user_question}")
        logger.info(f"Enhanced Question: {enhanced}")
        return enhanced
    except Exception as e:
        logger.error(f"Error enhancing question: {e}")
        return user_question  # fallback
