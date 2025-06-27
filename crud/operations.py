import sqlite3

DB_PATH = "/Users/yashbhoomkar/Desktop/BloombergProjects/ragasTest/v1/data/company.db"

def create_customer(first_name, last_name, email, country):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO customers (FirstName, LastName, Email, Country)
        VALUES (?, ?, ?, ?)
    """, (first_name, last_name, email, country))
    conn.commit()
    conn.close()

def read_customers():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT CustomerId, FirstName, LastName, Email, Country FROM customers")
    customers = cursor.fetchall()
    conn.close()
    return customers

def update_customer_email(customer_id, new_email):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE customers
        SET Email = ?
        WHERE CustomerId = ?
    """, (new_email, customer_id))
    conn.commit()
    conn.close()

def delete_customer(customer_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM customers WHERE CustomerId = ?", (customer_id,))
    conn.commit()
    conn.close()
