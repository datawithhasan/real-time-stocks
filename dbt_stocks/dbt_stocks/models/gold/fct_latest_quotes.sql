{{ config(materialized='table') }}

select
  symbol,
  market_ts,
  fetched_ts,
  close_price,
  open_price,
  high_price,
  low_price,
  prev_close_price,
  change_abs,
  change_pct
from {{ ref('slv_stock_quotes') }}
qualify row_number() over (
  partition by symbol
  order by market_ts desc, fetched_ts desc, ingested_at desc
) = 1