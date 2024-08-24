import threading
import time
import sys
import mysql.connector
import queue 

odd_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="",
        password=""
)

even_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="alanwalker"
)

def get_connection(user_id):
    # sharding key logic
    # here, we are using user_id and for odd user_ids one connection is used
    # and for even user_ids, choose the other connection
    if user_id % 2 == 0:
        print(f"user_id {user_id} gets even_connection")
        return even_connection
    else:
        print(f"user_id {user_id} gets odd_connection")
        return odd_connection

def run_query(data):
    user_id = data["user_id"]
    connection = get_connection(user_id)
    cursor = connection.cursor()
    cursor.execute("SELECT SLEEP(0.1)")
    cursor.fetchall()

def main():
    for i in range(10):
        run_query({"user_id": i})

if __name__ == "__main__":
    main()

# Output
# ➜  database-sharding git:(main) ✗ python3 sharding.py 
# user_id 0 gets even_connection
# user_id 1 gets odd_connection
# user_id 2 gets even_connection
# user_id 3 gets odd_connection
# user_id 4 gets even_connection
# user_id 5 gets odd_connection
# user_id 6 gets even_connection
# user_id 7 gets odd_connection
# user_id 8 gets even_connection
# user_id 9 gets odd_connection