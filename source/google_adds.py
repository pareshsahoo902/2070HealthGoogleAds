from google.ads.googleads.client import GoogleAdsClient
client = GoogleAdsClient.load_from_storage("google-ads.yaml")


ga_service = client.get_service("GoogleAdsService")

query = """
    SELECT ad_group_ad.ad.id, ad_group_ad.ad.image_ad.image_url FROM ad_group_ad
    ORDER BY ad_group_ad.ad.id"""

# Issues a search request using streaming.
stream = ga_service.search_stream(customer_id="1580734935", query=query)

for batch in stream:
    for row in batch.results:
        print(
            f"Campaign with ID {row.ad_group_ad.ad.id} and name: {row.ad_group_ad.ad.image_ad.image_url}"
        )