import mysql.connector
import json
from mysql.connector import Error

server = 'stg-nivaancare-mysql-01.cydlopxelbug.ap-south-1.rds.amazonaws.com'
database = 'nivaancare_production'
username = 'paresh.kumar'
password = 'aw102070eZiRey'


try:
# Establish a connection to the SQL Server database
    connection = mysql.connector.connect(
        host=server,
        user=username,
        password=password,
        database=database
    )

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
except Error as e:
    print(f"Error DB: {e}")
finally:
    # Perform any cleanup or additional actions here if needed
    connection.close()
    print("MySQL connection is closed")
    pass
