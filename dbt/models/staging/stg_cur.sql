with source as (
    select * from {{ source('finops_lab', 'cur_billing_data') }}
),

renamed as (
    select
        lineitemtype                                                as line_item_type,
        safe.parse_date('%Y-%m-%d', left(usagestartdate, 10))       as usage_start_date,
        billpayeraccountid                                          as bill_payer_account_id,
        usageaccountid                                              as usage_account_id,
        productcode                                                 as product_code,
        productfamily                                               as product_family,
        safe_cast(cost as FLOAT64)                                  as cost,
        resourceid                                                  as resource_id,
        region,
        _source_file                                                as source_file,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', _ingested_at)  as ingested_at
    from source
)

select * from renamed