with source as (
    select * from {{ source('finops_lab', 'server_metrics') }}
),

renamed as (
    select
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', timestamp)  as timestamp,
        server_id,
        safe_cast(cpu_usage as FLOAT64)                          as cpu_usage,
        safe_cast(memory_usage as FLOAT64)                       as memory_usage,
        safe_cast(disk_io_mbps as FLOAT64)                       as disk_io_mbps,
        safe_cast(network_in_mbps as FLOAT64)                    as network_in_mbps,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', _ingested_at) as ingested_at
    from source
)

select * from renamed