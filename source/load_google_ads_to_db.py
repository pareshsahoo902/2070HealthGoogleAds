import mysql.connector
import json
from mysql.connector import Error

server = 'stg-nivaancare-mysql-01.cydlopxelbug.ap-south-1.rds.amazonaws.com'
database = 'nivaancare_production'
username = 'paresh.kumar'
password = 'aw102070eZiRey'

def insertToDB(json_data,logger):
    # Establish a connection to the SQL Server database
    connection = mysql.connector.connect(
        host=server,
        user=username,
        password=password,
        database=database
    )
    logger.log_info(f"Connecting to {database} with host {server}")

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server")
        logger.log_info(f"Connected to MySQL Server : {db_Info}")

        cursor = connection.cursor()
        logger.log_info(f"Initiating Full Load : truncating table STG_GOOGLE_ADS")
        cursor.execute('truncate STG_GOOGLE_ADS')
        logger.log_info(f"STG_GOOGLE_ADS truncated.")
        # Define the SQL query to insert data into the table
        insert_query = """
        INSERT INTO STG_GOOGLE_ADS (account_id, account_name, ad_group_id, ad_id, ad_group_name, 
            campaign_resource_name, campaign_id, campaign_name, campaign_start_date, campaign_end_date, 
            clicks, conversions, cost, impressions, ctr, cost_per_conversion, average_cost, 
            average_cpc, conversions_from_interactions_rate,created_at,updated_at,created_by,updated_by,campaign_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s)
        """
        logger.log_info(f"Insert operation executed for {len(json_data)} \n Progress : \n")
        # Loop through the JSON data and insert each record into the table
        for idx, record in enumerate(json_data, start=1):
            values = tuple(record.values())
            cursor.execute(insert_query, values)
            if idx % 500 == 0:
                connection.commit()
                print(f"{idx} rows loaded and committed. ====>> Progress: {idx / len(json_data) * 100:.2f}/100")
                logger.log_info(f"{idx} rows loaded and committed. ====>> Progress: {idx / len(json_data) * 100:.2f}/100")

        # Commit the transaction and close the connection
        connection.commit()
        logger.log_info(f"Closing Connection with Database")

        connection.close()

def upsertToDB(json_data):
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
        upsert_query = """
        INSERT INTO STG_GOOGLE_ADS (
        account_id, account_name, ad_group_id, ad_id, ad_group_name,
        campaign_resource_name, campaign_id, campaign_name, campaign_start_date, campaign_end_date,
        clicks, conversions, cost, impressions, ctr, cost_per_conversion, average_cost,
        average_cpc, conversions_from_interactions_rate, created_at, updated_at, created_by, updated_by, campaign_date
        ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        campaign_start_date = VALUES(campaign_start_date),
        campaign_end_date = VALUES(campaign_end_date),
        clicks = VALUES(clicks),
        conversions = VALUES(conversions),
        cost = VALUES(cost),
        impressions = VALUES(impressions),
        ctr = VALUES(ctr),
        cost_per_conversion = VALUES(cost_per_conversion),
        average_cost = VALUES(average_cost),
        average_cpc = VALUES(average_cpc),
        conversions_from_interactions_rate = VALUES(conversions_from_interactions_rate),
        updated_at = VALUES(updated_at),
        updated_by = VALUES(updated_by)
        """

        for idx, record in enumerate(json_data, start=1):
            values = tuple(record.values())
            
            cursor.execute(upsert_query, values)
            if idx % 500 == 0:
                connection.commit()
                print(f"{idx} rows loaded and committed. ====>>  Progress: {idx / len(json_data) * 100:.2f}/100")

        # Commit the transaction and close the connection
        connection.commit()
        connection.close()