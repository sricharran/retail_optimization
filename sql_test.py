import mysql.connector
from mysql.connector import Error

# ✅ Correct Connection String
try:
    connection = mysql.connector.connect(
        host="localhost",  # ✅ REMOVE "2801@"
        user="root",  # 🔄 Replace with your MySQL username
        password="Charran@2801",  # 🔄 Replace with your MySQL password
        database="retail_optimization"  # 🔄 Ensure this database exists in MySQL
    )
    if connection.is_connected():
        print("✅ Connected to MySQL successfully!")

except Error as e:
    print(f"❌ Error connecting to MySQL: {e}")
