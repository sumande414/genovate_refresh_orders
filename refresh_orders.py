from flask import Flask, jsonify
from langchain_groq import ChatGroq
from groq import Groq
import pymysql
import json
import os
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()


app = Flask(__name__)


DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT"))
GROQ_API_KEY = os.getenv("GROQ_API_KEY")


llm = Groq(api_key=GROQ_API_KEY)


def get_db_connection():
    timeout = 10
    conn = pymysql.connect(
        charset="utf8mb4",
        connect_timeout=timeout,
        cursorclass=pymysql.cursors.DictCursor,
        db=DB_NAME,
        host=DB_HOST,
        password=DB_PASSWORD,
        read_timeout=timeout,
        port=DB_PORT,
        user=DB_USER,
        write_timeout=timeout,
    )
    return conn


def extract_data(text):
    prompt = f"""
    Extract the product name, quantity and address from the following text:
    
    Text: "{text}"
    
    Format the response as a JSON list of orders:
    [
        {{
            "product_name": "<product_name>",
            "quantity": "<quantity>",
            "address": "<address>"
        }},
        {{
            "product_name": "<product_name>",
            "quantity": "<quantity>",
            "address": "<address>"
        }}
    ]
    Do not include any extra text before or after the JSON output.
    """
    
    response = llm.chat.completions.create(
        model="llama3-70b-8192",
        messages=[{"role": "user", "content": prompt}]
    )
    

    print(f"API Response: {response}")
    
    json_response = response.choices[0].message.content.strip()
    
    if not json_response:
        print("Warning: Empty response received from the API.")
        return []
    
    try:
        orders = json.loads(json_response)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Response content: {json_response}")
        raise ValueError(f"Error decoding JSON: {e}\nResponse content: {json_response}")
    
    return orders


def fetch_unprocessed_emails():
    connection = get_db_connection()
    cursor = connection.cursor()

    query = "SELECT id, sender, body, email_date FROM RawEmails WHERE processed = 0"
    cursor.execute(query)
    emails = cursor.fetchall()

    cursor.close()
    connection.close()
    return emails


def insert_orders(order_data):
    connection = get_db_connection()
    cursor = connection.cursor()

    query = """
    INSERT INTO Orders (customer_email, product_name, quantity, address, date_of_order)
    VALUES (%s, %s, %s, %s, %s)
    """

    for order in order_data:
        cursor.execute(query, (
            order["customer_email"],
            order["product_name"],
            order["quantity"],
            order["address"],
            order["date_of_order"]
        ))

    connection.commit()
    cursor.close()
    connection.close()


def mark_email_as_processed(email_id, processed_status):
    connection = get_db_connection()
    cursor = connection.cursor()

    query = "UPDATE RawEmails SET processed = %s WHERE id = %s"
    cursor.execute(query, (processed_status, email_id))

    connection.commit()
    cursor.close()
    connection.close()


def process_orders():
    emails = fetch_unprocessed_emails()

    for email in emails:
        email_id = email["id"]
        sender = email["sender"]
        body = email["body"]
        date_of_order = email["email_date"].strftime("%Y-%m-%d") if isinstance(email["email_date"], datetime) else datetime.strptime(email["email_date"], "%b %d, %Y").strftime("%Y-%m-%d")

        try:
            extracted_orders = extract_data(body)

           
            for order in extracted_orders:
                order["customer_email"] = sender
                order["date_of_order"] = date_of_order

            
            print(f"Extracted Orders for {sender}:")
            for order in extracted_orders:
                print(json.dumps(order, indent=4))

            
            insert_orders(extracted_orders)

            
            mark_email_as_processed(email_id, 1)
            print(f"Email {email_id} processed successfully.")

        except Exception as e:
            
            print(f"Error processing email {email_id}: {e}")
            mark_email_as_processed(email_id, 0)


@app.route('/process-orders', methods=['GET'])
def api_process_orders():
    try:
       
        process_orders()

        response = jsonify({"message": "Orders processed successfully."})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response,200

    except Exception as e:
        response = jsonify({"error": str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response,500


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
