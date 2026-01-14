{{ config(materialized='table') }}

with base as (
  select
    symbol,
    date_trunc('day', market_ts) as trading_day,
    market_ts,
    open_price,
    high_price,
    low_price,
    close_price
  from {{ ref('slv_stock_quotes') }}
  where symbol is not null
    and market_ts is not null
),

ranked as (
  select
    *,
    row_number() over (partition by symbol, trading_day order by market_ts asc)  as rn_open,
    row_number() over (partition by symbol, trading_day order by market_ts desc) as rn_close
  from base
)

select
  symbol,
  trading_day,
  max(case when rn_open = 1 then open_price end)   as open_price,
  max(high_price)                                   as high_price,
  min(low_price)                                    as low_price,
  max(case when rn_close = 1 then close_price end)  as close_price,
  count(*)                                          as quotes_in_day
from ranked
group by 1,2