from google.ads.googleads.client import GoogleAdsClient
import json
from datetime import datetime
from load_google_ads_to_db import insertToDB,upsertToDB
from PyLogger import PyLogger

def convert_ad_group_status(status_code):
    # Define your status code to status text mapping
    status_mapping = {
        0: "PAUSED",
        1: "ENABLED",
        # Add more mappings as needed
    }
    return status_mapping.get(status_code, "UNKNOWN")

def convert_campaign_status(status_code):
    status_mapping = {
        0: "PAUSED",
        1: "ENABLED",
        2: "REMOVED",
        3: "ENDED",
        4: "UNKNOWN",
        # Add more mappings as needed
    }
    return status_mapping.get(status_code, "UNKNOWN")
def get_current_Date():
    current_datetime = datetime.now()

    return current_datetime.strftime('%Y-%m-%d %H:%M:%S')

def get_current_Day():
    current_datetime = datetime.now()

    return current_datetime.strftime('%Y-%m-%d')


def convert_google_ads_row_to_dict(google_ads_row):
    return {
        
            "account_id": google_ads_row.customer.id,
            "descriptive_name": google_ads_row.customer.descriptive_name,
            "ad_group_id": google_ads_row.ad_group.id,
            "ad_id": google_ads_row.ad_group_ad.ad.id,
            "ad_group_name": google_ads_row.ad_group.name,
            # "ad_group_status": convert_ad_group_status(google_ads_row.ad_group.status),
            "campaign_resource_name": google_ads_row.campaign.resource_name,
            "campaign_id": google_ads_row.campaign.id,
            "campaign_name": google_ads_row.campaign.name,
            # "campaign_status": convert_campaign_status(google_ads_row.campaign.primary_status),
            "campaign_start_date": google_ads_row.campaign.start_date,
            "campaign_end_date": google_ads_row.campaign.end_date,
            "clicks": google_ads_row.metrics.clicks,
            "conversions": google_ads_row.metrics.conversions,
            "cost_micros": google_ads_row.metrics.cost_micros,
            "impressions": google_ads_row.metrics.impressions,
            "ctr": google_ads_row.metrics.ctr,
            "cost_per_conversion": google_ads_row.metrics.cost_per_conversion,
            "average_cost": google_ads_row.metrics.average_cost,
            "average_cpc": google_ads_row.metrics.average_cpc,
            "conversions_from_interactions_rate": google_ads_row.metrics.conversions_from_interactions_rate,
            "created_at":get_current_Date(),
            "updated_at":get_current_Date(),
            "created_by" : "GoogleAdsAPI-PythonScript",
            "updated_by" : "GoogleAdsAPI-PythonScript",
            "campaign_date": google_ads_row.segments.date,
            
    }

def upload_to_db(converted_data, load_type,logger):
    try:
        if load_type == 'INSERT':
            insertToDB(converted_data,logger)
            logger.log_info(f"Data saved to DATABASE via INSERT Operation..")
            print(f"Data saved to DATABASE via INSERT Operation..")
        elif load_type == 'UPSERT':
            upsertToDB(converted_data)
            logger.log_info(f"Data saved to DATABASE via UPSERT Operation..")
            print(f"Data saved to DATABASE via INSERT Operation..")
        else:
            print(f"Invalid Load Type choose from : (INSERT/UPSERT)")

    except Exception as e:
        print(f"Error saving to DB: {e}")
    finally:
        # Perform any cleanup or additional actions here if needed
        pass




def save_to_json(converted_data, file_path):
    try:
        with open(file_path, 'w') as json_file:
            json.dump(converted_data, json_file, indent=2)
        print(f"Data saved to {file_path}")
    except Exception as e:
        print(f"Error saving to JSON: {e}")
    finally:
        # Perform any cleanup or additional actions here if needed
        pass

client = GoogleAdsClient.load_from_storage("google-ads.yaml")
ga_service = client.get_service("GoogleAdsService")

query = f"""
SELECT

ad_group_ad.ad.id,
ad_group_ad.ad.name,

ad_group.id,
ad_group.name,

campaign.id,
campaign.name,
campaign.primary_status,
campaign.start_date,
campaign.end_date,

metrics.average_cost,
metrics.cost_micros,
metrics.conversions,
metrics.cost_per_conversion,
metrics.conversions_from_interactions_rate,
metrics.average_cpc,
metrics.ctr,
metrics.clicks,
metrics.impressions,

customer.id,
customer.descriptive_name,

segments.date
FROM ad_group_ad
WHERE segments.date BETWEEN '2023-01-04' AND '{get_current_Day()}'
ORDER BY
campaign.id LIMIT 1500
"""

# Set the file path
file_path = "data/google_ads_data.json"
logger = PyLogger()
# Convert and save in batches
batch_size = 1000
converted_data = []

try:
    # Issues a search request using streaming.
    customer = "1580734935"
    stream = ga_service.search_stream(customer_id=customer, query=query)
    logger.log_info(f"Fetching Google ads Data for Customer ID : {customer}")

    for batch in stream:
        for row in batch.results:
            converted_data.append(convert_google_ads_row_to_dict(row))
            # if len(converted_data) >= batch_size:
            #     save_to_json(converted_data, file_path)
            #     upload_to_db(converted_data,"INSERT")
            #     converted_data = []

    # Save any remaining data in json or DB 
    #We can either Insert or Upsert data that is update and insert
    if converted_data:
        # save_to_json(converted_data, file_path)
        logger.log_info(f"Fetched {len(converted_data)} records from Google Ads.")
        logger.log_info(f"Uploading {len(converted_data)} records to STG DB")
        upload_to_db(converted_data,"INSERT",logger=logger)
    else:
        logger.log_info(f"No Google Ads Data Found Stopping Job...")
        

except Exception as e:
    print(f"An error occurred: {e}")
    logger.log_error(f"Error: {e}")
finally:
    logger.close_logger()
    pass
