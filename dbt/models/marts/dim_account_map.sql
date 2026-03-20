with staging as (
    select * from {{ ref('stg_account_map') }}
),

final as (
    select
        account_id,
        account_name,
        owner_email,
        business_unit,
        environment,
        primary_region,
        source_file,
        ingested_at,

        -- derived fields
        upper(environment)                          as environment_upper,
        upper(primary_region)                       as primary_region_upper,

        case
            when environment = 'production'         then true
            when environment = 'prod'               then true
            else false
        end as is_production

    from staging
)

select * from final