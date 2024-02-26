import mysql.connector
import json
from mysql.connector import Error

server = 'stg-nivaancare-mysql-01.cydlopxelbug.ap-south-1.rds.amazonaws.com'
database = 'nivaancare_production'
username = 'paresh.kumar'
password = 'aw102070eZiRey'

def load_to_DB(json_data):
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
        cursor = connection.cursor()

        # Define the SQL query to insert data into the table
        insert_query = """
        INSERT INTO STG_GOOGLE_ADS (account_id, account_name, ad_group_id, ad_id, ad_group_name, 
            campaign_resource_name, campaign_id, campaign_name, campaign_start_date, campaign_end_date, 
            clicks, conversions, cost, impressions, ctr, cost_per_conversion, average_cost, 
            average_cpc, conversions_from_interactions_rate,created_at,updated_at,created_by,updated_by,campaign_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s)
        """

        # Loop through the JSON data and insert each record into the table
        for record in json_data:
            values = tuple(record.values())
            cursor.execute(insert_query, values)

        # Commit the transaction and close the connection
        connection.commit()
        connection.close()