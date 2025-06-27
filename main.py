from crud.operations import (
    create_customer,
    read_customers,
    update_customer_email,
    delete_customer
)

def print_customers():
    customers = read_customers()
    for cust in customers:
        print(f"{cust[0]}: {cust[1]} {cust[2]}, {cust[3]}, {cust[4]}")

if __name__ == "__main__":
    print("💡 Current Customers:")
    print_customers()

    print("\n➕ Adding a new customer...")
    create_customer("Yash", "Bhoomkar", "yash@example.com", "India")
    print_customers()

    print("\n✏️ Updating email...")
    update_customer_email(60, "newemail@example.com")  # Replace 60 with a valid CustomerId
    print_customers()

    print("\n🗑️ Deleting the added customer...")
    delete_customer(60)  # Replace 60 with a valid CustomerId
    print_customers()
