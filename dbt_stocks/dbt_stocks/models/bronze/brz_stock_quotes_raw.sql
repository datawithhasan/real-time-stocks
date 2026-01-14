select
  raw,
  ingested_at,
  source,
  source_key,
  load_run_id
from {{ source('raw_stocks', 'STOCK_QUOTES_RAW') }}