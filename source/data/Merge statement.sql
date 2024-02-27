CREATE PROCEDURE MERGE_GOOGLE_ADS_DATA ()
BEGIN
	MERGE GOOGLE_ADS AS target
    USING STG_GOOGLE_ADS AS source
    ON target.campaign_id = source.campaign_id
       AND target.ad_group_id = source.ad_group_id
       AND target.ad_id = source.ad_id
    WHEN MATCHED THEN
        UPDATE SET
            account_id = source.account_id,
            account_name = source.account_name,
            ad_group_id = source.ad_group_id,
            ad_id = source.ad_id,
            ad_group_name = source.ad_group_name,
            campaign_resource_name = source.campaign_resource_name,
            campaign_name = source.campaign_name,
            campaign_start_date = source.campaign_start_date,
            campaign_end_date = source.campaign_end_date,
            clicks = source.clicks,
            conversions = source.conversions,
            cost = source.cost,
            impressions = source.impressions,
            ctr = source.ctr,
            cost_per_conversion = source.cost_per_conversion,
            average_cost = source.average_cost,
            average_cpc = source.average_cpc,
            conversions_from_interactions_rate = source.conversions_from_interactions_rate,
            created_at = source.created_at,
            updated_at = source.updated_at,
            created_by = source.created_by,
            updated_by = source.updated_by,
            campaign_date = source.campaign_date
    WHEN NOT MATCHED THEN
        INSERT (
            account_id, account_name, ad_group_id, ad_id, ad_group_name,
            campaign_resource_name, campaign_id, campaign_name, campaign_start_date, campaign_end_date,
            clicks, conversions, cost, impressions, ctr, cost_per_conversion, average_cost,
            average_cpc, conversions_from_interactions_rate, created_at, updated_at, created_by, updated_by, campaign_date
        )
        VALUES (
            source.account_id, source.account_name, source.ad_group_id, source.ad_id, source.ad_group_name,
            source.campaign_resource_name, source.campaign_id, source.campaign_name, source.campaign_start_date, source.campaign_end_date,
            source.clicks, source.conversions, source.cost, source.impressions, source.ctr,
            source.cost_per_conversion, source.average_cost, source.average_cpc, source.conversions_from_interactions_rate,
            source.created_at, source.updated_at, source.created_by, source.updated_by, source.campaign_date
        );
	
END