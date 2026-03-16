with staging as (
    select * from {{ ref('stg_server_metrics') }}
),

final as (
    select
        timestamp,
        server_id,
        cpu_usage,
        memory_usage,
        disk_io_mbps,
        network_in_mbps,
        ingested_at,

        -- derived metrics
        round(cpu_usage, 2)         as cpu_usage_pct,
        round(memory_usage, 2)      as memory_usage_pct,
        round(disk_io_mbps, 2)      as disk_io_mbps_rounded,
        round(network_in_mbps, 2)   as network_in_mbps_rounded,

        -- time partitioning helpers
        date(timestamp)             as metric_date,
        extract(hour from timestamp) as metric_hour

    from staging
)

select * from final