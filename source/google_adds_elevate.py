from google.ads.googleads.client import GoogleAdsClient
client = GoogleAdsClient.load_from_storage("google-ads.yaml")


ga_service = client.get_service("GoogleAdsService")

query = """
    SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        metrics.impressions
    FROM campaign
    WHERE segments.date DURING LAST_14_DAYS
    ORDER BY campaign.id"""

# Issues a search request using streaming.
stream = ga_service.search_stream(customer_id="5035506405", query=query)

for batch in stream:
    for row in batch.results:
        print(
            f"Campaign with ID {row.campaign.id} and name "

            f'"{row.campaign.name}" with status {row.campaign.status} was found with metric impressions {row.metrics.impressions}'
        )