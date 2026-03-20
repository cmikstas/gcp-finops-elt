with staging as (
    select * from {{ ref('stg_cur') }}
),

final as (
    select
        usage_start_date,
        bill_payer_account_id,
        usage_account_id,
        product_code,
        product_family,
        line_item_type,
        region,
        resource_id,
        source_file,
        ingested_at,

        -- cost metrics
        round(cost, 4)                                  as cost,

        -- time partitioning helpers
        date_trunc(usage_start_date, month)             as usage_month,
        extract(year from usage_start_date)             as usage_year,

        -- categorization
        case
            when product_code = 'AmazonEC2'         then 'Compute'
            when product_code = 'AmazonRDS'         then 'Database'
            when product_code = 'AmazonS3'          then 'Storage'
            when product_code = 'AWSLambda'         then 'Compute'
            when product_code = 'AmazonCloudFront'  then 'Network'
            when product_code = 'AmazonVPC'         then 'Network'
            else 'Other'
        end                                             as service_category

    from staging
    where line_item_type != 'Credit'
)

select * from final