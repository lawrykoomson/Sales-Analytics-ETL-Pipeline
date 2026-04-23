"""
Real-Time Sales Transaction Stream Simulator
=============================================
Simulates Apache Kafka-style real-time streaming
of sales transactions for Hubtel Ghana.

Architecture:
    Producer         → generates live sales events
    RevenueConsumer  → tracks high-value transactions
    MetricsConsumer  → aggregates real-time sales KPIs
    AuditConsumer    → logs all transactions to JSONL

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import queue
import threading
import time
import random
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("kafka_sales.log"),
        logging.StreamHandler()
    ]
)

TOPIC_NAME         = "sales.transactions.live"
PARTITION_COUNT    = 3
PRODUCER_RATE_HZ   = 10
SIMULATION_SECONDS = 60

CATEGORIES      = ["Electronics","Groceries","Clothing","Home & Garden","Health & Beauty","Sports","Automotive"]
REGIONS         = ["Greater Accra","Ashanti","Western","Eastern","Northern","Volta"]
CHANNELS        = ["In-Store","Online","Mobile App","Agent"]
PAYMENT_METHODS = ["MoMo","Cash","Card","Bank Transfer"]

REPORTS_PATH = Path("data/reports/")
REPORTS_PATH.mkdir(parents=True, exist_ok=True)


class SalesTopic:
    def __init__(self, name, partitions=3):
        self.name       = name
        self.partitions = [queue.Queue() for _ in range(partitions)]
        self.counter    = 0
        self.lock       = threading.Lock()

    def produce(self, msg):
        with self.lock:
            pid = self.counter % len(self.partitions)
            self.partitions[pid].put(msg)
            self.counter += 1

    def consume(self, pid, timeout=0.1):
        try:
            return self.partitions[pid].get(timeout=timeout)
        except queue.Empty:
            return None


class SalesTransactionProducer(threading.Thread):
    def __init__(self, topic, rate_hz, duration_secs):
        super().__init__(name="SalesProducer", daemon=True)
        self.topic    = topic
        self.rate_hz  = rate_hz
        self.duration = duration_secs
        self.produced = 0
        self.running  = True
        self.logger   = logging.getLogger("SalesProducer")
        self._counter = 1

    def generate_sale(self):
        unit_price = abs(random.lognormvariate(4.5, 1.0))
        quantity   = random.choices(range(1, 11), weights=[35,25,15,10,6,4,2,1,1,1])[0]
        discount   = random.choices([0,0.05,0.10,0.15,0.20,0.25],
                                    weights=[75,10,6,4,3,2])[0]
        gross      = round(unit_price * quantity, 2)
        disc_amt   = round(gross * discount, 2)
        net        = round(gross - disc_amt, 2)
        profit     = round(net * random.uniform(0.30, 0.55), 2)

        return {
            "event_id":       f"EVT-{str(self._counter).zfill(8)}",
            "transaction_id": f"SALE-LIVE-{str(self._counter).zfill(9)}",
            "timestamp":      datetime.now().isoformat(),
            "category":       random.choices(CATEGORIES, weights=[20,25,15,12,12,10,6])[0],
            "region":         random.choices(REGIONS,    weights=[35,25,15,12,8,5])[0],
            "channel":        random.choices(CHANNELS,   weights=[35,30,25,10])[0],
            "payment_method": random.choices(PAYMENT_METHODS, weights=[45,25,20,10])[0],
            "unit_price_ghs": round(unit_price, 2),
            "quantity":       quantity,
            "discount_pct":   discount,
            "gross_revenue":  gross,
            "net_revenue":    net,
            "gross_profit":   profit,
            "is_high_value":  net >= 500,
            "is_discounted":  discount > 0,
        }

    def run(self):
        self.logger.info(f"Producer started on topic '{self.topic.name}' at {self.rate_hz} sales/sec")
        end_time   = time.time() + self.duration
        sleep_time = 1.0 / self.rate_hz
        while self.running and time.time() < end_time:
            self.topic.produce(self.generate_sale())
            self.produced  += 1
            self._counter  += 1
            time.sleep(sleep_time)
        self.running = False
        self.logger.info(f"Producer finished — published {self.produced:,} sales events")


class RevenueConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="RevenueConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.alerts   = []
        self.logger   = logging.getLogger("RevenueConsumer")

    def run(self):
        self.logger.info("Consumer started — tracking high-value sales on partition 0")
        while self.running:
            msg = self.topic.consume(0)
            if msg is None:
                continue
            if msg["is_high_value"]:
                self.alerts.append(msg)
                self.logger.info(
                    f"HIGH VALUE SALE | {msg['transaction_id']} | "
                    f"GHS {msg['net_revenue']:,.2f} | "
                    f"{msg['category']} | {msg['channel']}"
                )


class MetricsConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="MetricsConsumer", daemon=True)
        self.topic   = topic
        self.running = True
        self.logger  = logging.getLogger("MetricsConsumer")
        self.m = {
            "total": 0, "high_value": 0, "discounted": 0,
            "total_revenue": 0.0, "total_profit": 0.0,
            "by_category": {}, "by_channel": {}, "by_region": {}
        }

    def run(self):
        self.logger.info("Consumer started — aggregating metrics on partition 1")
        while self.running:
            msg = self.topic.consume(1)
            if msg is None:
                continue
            m = self.m
            m["total"]         += 1
            m["total_revenue"] += msg["net_revenue"]
            m["total_profit"]  += msg["gross_profit"]
            if msg["is_high_value"]:  m["high_value"]  += 1
            if msg["is_discounted"]:  m["discounted"]  += 1
            m["by_category"][msg["category"]] = \
                m["by_category"].get(msg["category"], 0) + msg["net_revenue"]
            m["by_channel"][msg["channel"]] = \
                m["by_channel"].get(msg["channel"], 0) + msg["net_revenue"]
            m["by_region"][msg["region"]] = \
                m["by_region"].get(msg["region"], 0) + msg["net_revenue"]

    def snapshot(self):
        m = self.m
        return {
            "total":          m["total"],
            "high_value":     m["high_value"],
            "discounted":     m["discounted"],
            "total_revenue":  round(m["total_revenue"], 2),
            "total_profit":   round(m["total_profit"], 2),
            "top_category":   max(m["by_category"], key=m["by_category"].get, default="N/A"),
            "top_channel":    max(m["by_channel"],  key=m["by_channel"].get,  default="N/A"),
            "top_region":     max(m["by_region"],   key=m["by_region"].get,   default="N/A"),
        }


class AuditConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="AuditConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.consumed = 0
        self.logger   = logging.getLogger("AuditConsumer")
        self.log_file = REPORTS_PATH / "sales_events_live.jsonl"

    def run(self):
        self.logger.info(f"Consumer started — logging all sales to {self.log_file}")
        with open(self.log_file, "w") as f:
            while self.running:
                msg = self.topic.consume(2)
                if msg is None:
                    continue
                self.consumed += 1
                f.write(json.dumps(msg) + "\n")
                f.flush()


def print_live_metrics(producer, metrics, revenue, audit, interval=10):
    start = time.time()
    while producer.running:
        time.sleep(interval)
        elapsed = int(time.time() - start)
        snap    = metrics.snapshot()
        print("\n" + "="*65)
        print(f"  SALES STREAM — LIVE METRICS  [{elapsed}s elapsed]")
        print("="*65)
        print(f"  Sales Produced        : {producer.produced:,}")
        print(f"  Throughput            : {producer.produced/max(elapsed,1):.1f} sales/sec")
        print(f"  Total Scored          : {snap['total']:,}")
        print(f"  High Value Sales      : {snap['high_value']:,}")
        print(f"  Discounted Sales      : {snap['discounted']:,}")
        print(f"  Total Revenue         : GHS {snap['total_revenue']:,.2f}")
        print(f"  Total Profit          : GHS {snap['total_profit']:,.2f}")
        print(f"  Top Category          : {snap['top_category']}")
        print(f"  Top Channel           : {snap['top_channel']}")
        print(f"  Top Region            : {snap['top_region']}")
        print(f"  High Value Alerts     : {len(revenue.alerts):,}")
        print(f"  Events Logged         : {audit.consumed:,}")
        print("="*65)


def run_kafka_sales_simulator():
    print("\n" + "="*65)
    print("  HUBTEL GHANA — SALES KAFKA STREAM SIMULATOR")
    print("  Architecture: Producer → Topic → 3 Consumer Groups")
    print("="*65)
    print(f"  Topic          : {TOPIC_NAME}")
    print(f"  Partitions     : {PARTITION_COUNT}")
    print(f"  Producer Rate  : {PRODUCER_RATE_HZ} sales/sec")
    print(f"  Duration       : {SIMULATION_SECONDS} seconds")
    print(f"  Expected       : ~{PRODUCER_RATE_HZ * SIMULATION_SECONDS:,} sales")
    print("="*65 + "\n")

    topic   = SalesTopic(TOPIC_NAME, PARTITION_COUNT)
    producer = SalesTransactionProducer(topic, PRODUCER_RATE_HZ, SIMULATION_SECONDS)
    revenue  = RevenueConsumer(topic)
    metrics  = MetricsConsumer(topic)
    audit    = AuditConsumer(topic)

    for t in [producer, revenue, metrics, audit]:
        t.start()

    m_thread = threading.Thread(
        target=print_live_metrics,
        args=(producer, metrics, revenue, audit, 10),
        daemon=True
    )
    m_thread.start()
    producer.join()
    time.sleep(3)
    for t in [revenue, metrics, audit]:
        t.running = False

    final = metrics.snapshot()
    print("\n" + "="*65)
    print("  SALES KAFKA SIMULATION — FINAL SUMMARY")
    print("="*65)
    print(f"  Total Sales Produced   : {producer.produced:,}")
    print(f"  High Value Sales       : {final['high_value']:,}")
    print(f"  Discounted Sales       : {final['discounted']:,}")
    print(f"  Total Revenue Streamed : GHS {final['total_revenue']:,.2f}")
    print(f"  Total Profit Streamed  : GHS {final['total_profit']:,.2f}")
    print(f"  High Value Alerts      : {len(revenue.alerts):,}")
    print(f"  Top Category           : {final['top_category']}")
    print(f"  Top Channel            : {final['top_channel']}")
    print(f"  Top Region             : {final['top_region']}")
    print(f"  Events Logged          : {audit.consumed:,}")
    print("="*65 + "\n")

    if revenue.alerts:
        import csv
        alerts_path = REPORTS_PATH / "high_value_sales_alerts.csv"
        with open(alerts_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=revenue.alerts[0].keys())
            writer.writeheader()
            writer.writerows(revenue.alerts)
        print(f"  High value alerts saved: {alerts_path}")


if __name__ == "__main__":
    run_kafka_sales_simulator()