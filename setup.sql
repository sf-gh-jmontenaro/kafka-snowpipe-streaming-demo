-- Landing table for the Kafka -> Snowpipe Streaming demo.
-- Typed columns (AMOUNT, APPLICATION_DATE) exist so a bad value gets rejected
-- into the error table; PAYLOAD keeps the full scrubbed event.
CREATE DATABASE IF NOT EXISTS SNOWPIPE_STREAMING;
CREATE SCHEMA IF NOT EXISTS SNOWPIPE_STREAMING.PUBLIC;

CREATE OR REPLACE TABLE SNOWPIPE_STREAMING.PUBLIC.RAW_EVENTS (
    EVENT_ID          STRING,
    APPLICATION_PK    STRING,
    APPLICATION_DATE  DATE,
    TXN_TYPE          STRING,
    AMOUNT            NUMBER(12,2),
    PAYLOAD           VARIANT,
    KAFKA_TOPIC       STRING,
    KAFKA_PARTITION   NUMBER,
    KAFKA_OFFSET      NUMBER,
    CONSUMER_INSTANCE STRING
)
ERROR_LOGGING = TRUE;
-- The default pipe "RAW_EVENTS-STREAMING" is created automatically on first use.
