from google.ads.googleads.client import GoogleAdsClient
client = GoogleAdsClient.load_from_storage("google-ads.yaml")
import json



def convert_google_ads_row_to_dict(google_ads_row):
    converted_data = {
        "ad_group": {
            "resource_name": google_ads_row.ad_group.resource_name,
            "id": google_ads_row.ad_group.id,
            "name": google_ads_row.ad_group.name,
            "status": google_ads_row.ad_group.status,
            "campaign": google_ads_row.ad_group.campaign,
        },
        "campaign": {
            "resource_name": google_ads_row.campaign.resource_name,
            "id": google_ads_row.campaign.id,
            "name": google_ads_row.campaign.name,
            "start_date":google_ads_row.campaign.start_date,
            "end_date":google_ads_row.campaign.end_date,

        },
        "metrics": {
            "active_view_impressions": google_ads_row.metrics.active_view_impressions,
            "clicks": google_ads_row.metrics.clicks,
            "conversions": google_ads_row.metrics.conversions,
            "cost_micros": google_ads_row.metrics.cost_micros,
            "impressions": google_ads_row.metrics.impressions,
        },
    }

    return converted_data

ga_service = client.get_service("GoogleAdsService")

query = """
SELECT

ad_group_ad.ad.id,
ad_group_ad.ad.name,

ad_group.id,
ad_group.name,

campaign.id,
campaign.experiment_type,
campaign.name,
campaign.primary_status,

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
WHERE segments.date BETWEEN '2023-01-04' AND '2024-02-23'
    """
# Issues a search request using streaming.
stream = ga_service.search_stream(customer_id="1580734935", query=query)

converted_data = []

for batch in stream:
    for row in batch.results:
        print(row)
        

# file_path = "data/google_ads_data.json"
# with open(file_path, 'w') as json_file:
#     json.dump(converted_data, json_file,indent=2)


# print(f"Data saved to {file_path}")