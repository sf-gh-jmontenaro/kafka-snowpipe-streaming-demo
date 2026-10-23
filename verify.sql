-- Verification queries for the demo.
USE SCHEMA SNOWPIPE_STREAMING.PUBLIC;

-- 1. Rows per partition, and who loaded them
SELECT KAFKA_PARTITION, CONSUMER_INSTANCE, COUNT(*) AS ROWS_LOADED,
       MIN(KAFKA_OFFSET) AS MIN_OFF, MAX(KAFKA_OFFSET) AS MAX_OFF
FROM RAW_EVENTS GROUP BY 1, 2 ORDER BY 1, 2;

-- 2. Duplicates (expect zero rows)
SELECT KAFKA_PARTITION, KAFKA_OFFSET, COUNT(*) AS N
FROM RAW_EVENTS GROUP BY 1, 2 HAVING COUNT(*) > 1;

-- 3. Offset gaps (expect only the offsets that went to DLQ or the error table)
SELECT KAFKA_PARTITION, PREV_OFFSET + 1 AS GAP_START, KAFKA_OFFSET - 1 AS GAP_END
FROM (
  SELECT KAFKA_PARTITION, KAFKA_OFFSET,
         LAG(KAFKA_OFFSET) OVER (PARTITION BY KAFKA_PARTITION ORDER BY KAFKA_OFFSET) AS PREV_OFFSET
  FROM RAW_EVENTS
)
WHERE KAFKA_OFFSET - PREV_OFFSET > 1
ORDER BY 1, 2;

-- 4. PII check (expect zero)
SELECT COUNT(*) AS ROWS_WITH_PII
FROM RAW_EVENTS
WHERE PAYLOAD:ssn IS NOT NULL OR PAYLOAD:full_name IS NOT NULL OR PAYLOAD:dob IS NOT NULL;

-- 5. Rejected rows captured by Snowflake.
--    Metadata offset bounds are per-batch; the exact Kafka offset comes from the
--    original row, stored as a JSON string in ERROR_DATA:"$1".
SELECT ERROR_CODE,
       ERROR_METADATA:error_source::STRING                  AS FAILED_COLUMN,
       ERROR_METADATA:error_message::STRING                 AS MESSAGE,
       ERROR_METADATA:details.channel_name::STRING          AS CHANNEL,
       ERROR_METADATA:details.offset_token_lower_bound::STRING AS BATCH_TOKEN_LO,
       ERROR_METADATA:details.offset_token_upper_bound::STRING AS BATCH_TOKEN_HI,
       PARSE_JSON(ERROR_DATA:"$1"):KAFKA_PARTITION::INT     AS KAFKA_PARTITION,
       PARSE_JSON(ERROR_DATA:"$1"):KAFKA_OFFSET::INT        AS KAFKA_OFFSET,
       PARSE_JSON(ERROR_DATA:"$1")                          AS ORIGINAL_ROW
FROM ERROR_TABLE(RAW_EVENTS)
ORDER BY KAFKA_PARTITION, KAFKA_OFFSET;

-- 6. Channel history (ACCOUNT_USAGE: latency of minutes+; appears to record error
--    events, so channels without errors may not show up). Each table/pipe recreate
--    gets a new PIPE_ID -- tokens belong to that pipe instance.
--    For live committed tokens use the SDK: client.get_channel_statuses([...]).
--    (SHOW CHANNELS returns nothing for high-performance channels.)
SELECT CHANNEL_NAME, PIPE_ID, PIPE_NAME, CREATED_ON, END_OFFSET,
       ROWS_INSERTED, ROWS_PARSED, ROW_ERROR_COUNT,
       LAST_ERROR_OFFSET_UPPER_BOUND, LAST_ERROR_MESSAGE
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY
WHERE TABLE_DATABASE_NAME = 'SNOWPIPE_STREAMING'
  AND TABLE_SCHEMA_NAME = 'PUBLIC'
  AND TABLE_NAME = 'RAW_EVENTS'
QUALIFY ROW_NUMBER() OVER (PARTITION BY PIPE_ID, CHANNEL_NAME ORDER BY CREATED_ON DESC) = 1
ORDER BY PIPE_ID, CHANNEL_NAME;

-- Reset between runs: recreating the table also recreates the pipe and wipes all
-- channel tokens, so the consumer replays the whole topic. Reset Kafka at the same
-- time (see README), or the DLQ gets duplicate entries.
