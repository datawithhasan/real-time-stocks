with src as (
  select * from {{ ref('brz_stock_quotes_raw') }}
),
flattened as (
  select
    raw:symbol::string                       as symbol,
    raw:c::float                             as close_price,
    raw:o::float                             as open_price,
    raw:h::float                             as high_price,
    raw:l::float                             as low_price,
    raw:pc::float                            as prev_close_price,
    raw:d::float                             as change_abs,
    raw:dp::float                            as change_pct,
    to_timestamp_ntz(raw:t::number)          as market_ts,
    to_timestamp_ntz(raw:fetched_at::number) as fetched_ts,
    ingested_at,
    source,
    source_key,
    load_run_id
  from src
)
select *
from flattened
qualify row_number() over (
  partition by symbol, market_ts, fetched_ts
  order by ingested_at desc
) = 1