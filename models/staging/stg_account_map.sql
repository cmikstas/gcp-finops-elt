with source as (
    select * from {{ source('finops_lab', 'account_map') }}
),

renamed as (
    select
        account_id,
        account_name,
        owner_email,
        business_unit,
        environment,
        primary_region,
        _source_file as source_file,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', _ingested_at) as ingested_at
    from source
)

select * from renamed