"""Kafka -> PII scrub -> Snowpipe Streaming (high-performance), Option B:
one named channel per Kafka partition, Snowflake holds the checkpoint.

  python consumer.py --instance a
  python consumer.py --instance b                  # second instance -> rebalance
  python consumer.py --instance a --crash-after 1500   # hard kill mid-stream

Bounded replay (runs alongside the live consumer; delete the range in Snowflake first):
  python consumer.py --instance replay1 --replay 0:1200-1500 --replay 3:980-1100
"""
import argparse
import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, OFFSET_BEGINNING, Producer, TopicPartition
from snowflake.ingest.streaming import StreamingIngestClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logging.getLogger("snowflake").setLevel(logging.WARNING)

TOPIC = "demo-events"
DLQ_TOPIC = "demo-events-dlq"
PII_FIELDS = {"full_name", "ssn", "dob"}
REPORT_EVERY_SEC = 10


class KafkaToSnowflake:
    def __init__(self, instance: str, profile_json: str, replay: dict = None):
        self.instance = instance
        self.replay = replay or {}   # partition -> (start, end), inclusive
        self.log = logging.getLogger(f"consumer-{instance}")
        kafka_conf = {"bootstrap.servers": "localhost:29092"}
        self.consumer = Consumer({
            **kafka_conf,
            # Replay uses its own group so it never joins (and rebalances) the live group
            "group.id": f"stream-replay-{instance}" if self.replay else "stream-consumer",
            "enable.auto.commit": False,      # Snowflake holds the checkpoint
            "auto.offset.reset": "earliest",
        })
        self.dlq = Producer(kafka_conf)
        self.client = StreamingIngestClient.from_table(
            client_name=f"stream-consumer-{instance}",
            db_name="SNOWPIPE_STREAMING",
            schema_name="PUBLIC",
            table_name="RAW_EVENTS",
            profile_json=profile_json,
        )
        self.channels = {}   # kafka partition -> channel
        self.running = True
        self.processed = 0
        self.last_appended = {}   # replay: partition -> last offset appended
        self.replay_done = set()

    # ---- rebalance callbacks ------------------------------------------------

    def _channel_name(self, partition: int) -> str:
        if self.replay:
            # Separate channels: reusing the live name would supersede (409) the live consumer
            return f"{TOPIC}_replay_{self.instance}_p{partition}"
        # Stable across restarts/instances: whoever owns the partition owns the channel
        return f"{TOPIC}_p{partition}"

    def on_assign(self, consumer, partitions):
        for tp in partitions:
            channel, _ = self.client.open_channel(self._channel_name(tp.partition))
            token = channel.get_latest_committed_offset_token()
            tp.offset = int(token) + 1 if token is not None else OFFSET_BEGINNING
            self.channels[tp.partition] = channel
            self.log.info(f"ASSIGNED p{tp.partition}: committed={token} -> seek {tp.offset}")
        consumer.assign(partitions)   # seek happens here via tp.offset

    def on_revoke(self, consumer, partitions):
        for tp in partitions:
            channel = self.channels.pop(tp.partition, None)
            if channel:
                channel.close(wait_for_flush=True, timeout_seconds=30)
                self.log.info(f"REVOKED p{tp.partition}: flushed and closed channel")

    # ---- per-record processing ----------------------------------------------

    def _to_row(self, msg) -> dict:
        payload = json.loads(msg.value())
        for field in PII_FIELDS:
            payload.pop(field, None)
        return {
            "EVENT_ID": payload["event_id"],
            "APPLICATION_PK": payload["application_pk"],
            "APPLICATION_DATE": payload["application_date"],
            "TXN_TYPE": payload["txn_type"],
            "AMOUNT": payload["amount"],         # a non-numeric value is rejected server-side
            "PAYLOAD": payload,
            "KAFKA_TOPIC": msg.topic(),          # metadata in every row: makes error
            "KAFKA_PARTITION": msg.partition(),  # rows traceable and enables
            "KAFKA_OFFSET": msg.offset(),        # gap reconciliation
            "CONSUMER_INSTANCE": self.instance,
        }

    def _to_dlq(self, msg, error: Exception):
        self.dlq.produce(DLQ_TOPIC, value=msg.value(), headers={
            "error": f"{type(error).__name__}: {error}",
            "source_partition": str(msg.partition()),
            "source_offset": str(msg.offset()),
        })
        self.log.warning(f"DLQ p{msg.partition()}@{msg.offset()}: {error}")

    def process(self, msg):
        try:
            row = self._to_row(msg)
        except Exception as e:                   # category 1: bad input
            self._to_dlq(msg, e)
            return
        # Token = Kafka offset. Snowflake stores the latest committed one per channel.
        self.channels[msg.partition()].append_row(row, offset_token=str(msg.offset()))
        self.last_appended[msg.partition()] = msg.offset()

    # ---- reporting ----------------------------------------------------------

    def _log_status(self, p: int, s):
        self.log.info(
            f"p{p}: status={s.status_code} committed={s.latest_committed_offset_token} "
            f"inserted={s.rows_inserted_count} errors={s.rows_error_count}"
        )

    def report(self):
        # Rejected rows are acknowledged normally; rows_error_count is the signal.
        commits = []
        for p, ch in sorted(self.channels.items()):
            s = ch.get_channel_status()
            self._log_status(p, s)
            if s.latest_committed_offset_token is not None:
                commits.append(TopicPartition(TOPIC, p, int(s.latest_committed_offset_token) + 1))
        # Informational only (consumer-lag dashboards); resume point still comes from Snowflake.
        if commits and not self.replay:
            self.consumer.commit(offsets=commits, asynchronous=True)

    # ---- bounded replay ------------------------------------------------------

    def start_replay(self):
        assignments = []
        for p, (start, end) in sorted(self.replay.items()):
            low, high = self.consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
            if start < low:
                raise SystemExit(f"p{p}: start {start} is below retained offset {low}")
            if end >= high:
                raise SystemExit(f"p{p}: end {end} is beyond latest offset {high - 1}")
            channel, _ = self.client.open_channel(self._channel_name(p))
            token = channel.get_latest_committed_offset_token()
            seek = int(token) + 1 if token is not None else start   # resume if this replay crashed
            self.channels[p] = channel
            if seek > end:
                self.replay_done.add(p)
                self.log.info(f"REPLAY p{p}: already complete (committed={token})")
                continue
            assignments.append(TopicPartition(TOPIC, p, seek))
            self.log.info(f"REPLAY p{p}: offsets {start}-{end}, seek {seek}")
        self.consumer.assign(assignments)   # assign, not subscribe: no group membership

    def replay_check(self, msg) -> bool:
        """Return True if msg is inside its partition's replay range."""
        p = msg.partition()
        end = self.replay[p][1]
        if msg.offset() >= end:
            self.consumer.pause([TopicPartition(TOPIC, p)])
            self.replay_done.add(p)
            if len(self.replay_done) == len(self.replay):
                self.running = False
        return msg.offset() <= end

    def finish_replay(self):
        for p, ch in sorted(self.channels.items()):
            last = self.last_appended.get(p)
            if last is not None:
                ch.wait_for_commit(lambda t, last=last: t is not None and int(t) >= last,
                                   timeout_seconds=120)
            s = ch.get_channel_status()
            self._log_status(p, s)
            self.log.info(f"REPLAY p{p}: done, dropping replay channel")
            ch.close(drop=True)   # throwaway channel; lets the same --instance replay again
        self.channels.clear()

    # ---- main loop -----------------------------------------------------------

    def run(self, crash_after: int = 0):
        if self.replay:
            self.start_replay()
            if len(self.replay_done) == len(self.replay):
                self.running = False
        else:
            self.consumer.subscribe([TOPIC], on_assign=self.on_assign, on_revoke=self.on_revoke)
        last_report = time.time()
        while self.running:
            msg = self.consumer.poll(1.0)
            if msg is not None and not msg.error():
                if self.replay and not self.replay_check(msg):
                    continue
                self.process(msg)
                self.processed += 1
                if crash_after and self.processed >= crash_after:
                    self.log.error(f"SIMULATED CRASH after {self.processed} records (no flush)")
                    os._exit(1)
            elif msg is not None:
                self.log.error(f"Kafka error: {msg.error()}")
            self.dlq.poll(0)
            if time.time() - last_report >= REPORT_EVERY_SEC:
                self.report()
                last_report = time.time()

    def shutdown(self):
        self.log.info("Shutting down: flushing channels")
        if self.replay and len(self.replay_done) == len(self.replay):
            self.finish_replay()
        self.on_revoke(self.consumer, [TopicPartition(TOPIC, p) for p in list(self.channels)])
        self.dlq.flush(10)
        self.consumer.close()
        self.client.close(wait_for_flush=True, timeout_seconds=60)


def parse_replay(specs) -> dict:
    """['0:1200-1500', '3:980-1100'] -> {0: (1200, 1500), 3: (980, 1100)}"""
    ranges = {}
    for spec in specs or []:
        part, rng = spec.split(":")
        start, end = (int(x) for x in rng.split("-"))
        if start > end:
            raise SystemExit(f"bad --replay {spec}: start > end")
        ranges[int(part)] = (start, end)
    return ranges


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--instance", default="a")
    ap.add_argument("--profile", default=os.path.join(os.path.dirname(__file__), "profile.json"))
    ap.add_argument("--crash-after", type=int, default=0)
    ap.add_argument("--replay", action="append", metavar="PARTITION:START-END",
                    help="replay an inclusive offset range; repeat per partition")
    args = ap.parse_args()

    app = KafkaToSnowflake(args.instance, args.profile, replay=parse_replay(args.replay))

    def stop(*_):
        app.running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        app.run(crash_after=args.crash_after)
    finally:
        app.shutdown()


if __name__ == "__main__":
    main()
