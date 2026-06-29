"""
PySpark Day 2 — RDD Exercises
==============================
3 exercises, increasing difficulty.
Each exercise has a dataset, your TODOs, and expected output.
Do NOT use DataFrames. Raw RDD API only.
"""

from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .master("local[*]")
    .appName("Day2-RDD-Exercises")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext


# ══════════════════════════════════════════════════════════════
# EXERCISE 1 — Word frequency counter
# Difficulty: Basic
# Concepts: flatMap, map, reduceByKey, sortBy
# ══════════════════════════════════════════════════════════════
"""
Dataset: 5 log lines from a Spark job
Goal: Count how many times each word appears. Print top 5.

Steps you need to chain:
  1. flatMap — split each line into words
  2. map     — turn each word into a (word, 1) tuple
  3. reduceByKey — sum the counts
  4. sortBy  — sort by count descending
  5. take(5) — get top 5
"""

logs = sc.parallelize([
    "spark job started executor executor started",
    "executor failed spark restarting executor",
    "spark job completed successfully job done",
    "executor memory exceeded spark killing executor",
    "spark job submitted spark cluster running",
])

# TODO 1: implement the word frequency chain
# word_counts = logs \
#     .flatMap(???) \
#     .map(???) \
#     .reduceByKey(???) \
#     .sortBy(???, ascending=False) \
#     .take(5)
# print("Top 5 words:", word_counts)

# Expected output (order within same count may vary):
# Top 5 words:
# [('spark', 6), ('executor', 6), ('job', 4), ('started', 2), ('failed', 1)]
# Note: 'spark' and 'executor' both appear 6 times.


# ══════════════════════════════════════════════════════════════
# EXERCISE 2 — Sensor data aggregation
# Difficulty: Intermediate
# Concepts: map, filter, reduceByKey, mapValues
# ══════════════════════════════════════════════════════════════
"""
Dataset: Raw sensor readings as strings (like a textFile result)
Format: "cell_id,temp_c,voltage,status"
Goal:
  a) Parse the raw strings into typed tuples
  b) Filter out any rows where status == "FAULT"
  c) Compute average temperature per cell_id
  d) Print results sorted by cell_id

Hint for average: you can't reduceByKey directly on a float average.
Track (sum, count) per key, then divide at the end with mapValues.
"""

raw_sensor = sc.parallelize([
    "FC-01,720.5,0.78,OK",
    "FC-01,735.2,0.75,OK",
    "FC-01,698.1,0.81,FAULT",
    "FC-02,715.0,0.79,OK",
    "FC-02,729.8,0.76,OK",
    "FC-02,701.3,0.80,OK",
    "FC-03,740.2,0.74,FAULT",
    "FC-03,755.1,0.71,OK",
    "FC-03,688.4,0.83,OK",
])

# TODO 2a: parse each line into (cell_id, temp_c, voltage, status)
# parsed = raw_sensor.map(lambda line: ???)

# TODO 2b: filter out FAULT rows
# clean = parsed.filter(???)

# TODO 2c: map to key-value pairs for aggregation
# For average, map to: (cell_id, (temp_c, 1))
# Then reduceByKey to accumulate (sum_temp, count)
# Then mapValues to divide sum/count
# avg_temp = clean \
#     .map(???) \
#     .reduceByKey(???) \
#     .mapValues(???)

# TODO 2d: sort and print
# for row in sorted(avg_temp.collect()):
#     print(f"{row[0]}: avg_temp = {row[1]:.2f}°C")

# Expected output (FAULT rows excluded):
# FC-01: avg_temp = 727.85°C   (only 2 OK rows: 720.5, 735.2)
# FC-02: avg_temp = 715.37°C   (all 3 OK)
# FC-03: avg_temp = 721.75°C   (only 2 OK rows: 755.1, 688.4)


# ══════════════════════════════════════════════════════════════
# EXERCISE 3 — Fleet health score
# Difficulty: Hard
# Concepts: map, filter, reduceByKey, join, sortBy
# ══════════════════════════════════════════════════════════════
"""
Two datasets:
  - readings: (cell_id, temp_c, voltage)
  - thresholds: (cell_id, max_temp, min_voltage)  ← per-cell limits

Goal:
  a) For each reading, check if temp_c > max_temp OR voltage < min_voltage
     → mark it as a "violation" (1) or not (0)
  b) Count total readings and total violations per cell
  c) Compute violation_rate = violations / total_readings
  d) Print each cell with its violation rate, sorted worst to best

This requires joining two RDDs — use RDD join().
"""

readings = sc.parallelize([
    ("FC-01", 720.5, 0.78),
    ("FC-01", 760.0, 0.65),   # both violated: temp > 740, volt < 0.70
    ("FC-01", 698.1, 0.81),
    ("FC-02", 715.0, 0.79),
    ("FC-02", 750.0, 0.68),   # both violated
    ("FC-02", 701.3, 0.80),
    ("FC-03", 740.2, 0.74),   # temp violated: > 730
    ("FC-03", 755.1, 0.71),   # temp violated
    ("FC-03", 688.4, 0.83),
])

# Per-cell thresholds: (cell_id, max_temp, min_voltage)
thresholds = sc.parallelize([
    ("FC-01", 740.0, 0.70),
    ("FC-02", 730.0, 0.72),
    ("FC-03", 730.0, 0.72),
])

# TODO 3a: convert thresholds to key-value RDD for join
# threshold_kv = thresholds.map(lambda x: (x[0], (x[1], x[2])))

# TODO 3b: convert readings to key-value RDD
# readings_kv = readings.map(lambda x: (x[0], (x[1], x[2])))

# TODO 3c: join readings with thresholds on cell_id
# joined will be: (cell_id, ((temp_c, voltage), (max_temp, min_voltage)))
# joined = readings_kv.join(threshold_kv)

# TODO 3d: map each joined row to (cell_id, (is_violation, 1))
# is_violation = 1 if temp > max_temp OR voltage < min_voltage else 0
# flagged = joined.map(lambda x: ???)

# TODO 3e: reduceByKey to accumulate (total_violations, total_readings)
# stats = flagged.reduceByKey(lambda a, b: ???)

# TODO 3f: compute violation rate and sort
# result = stats \
#     .mapValues(lambda v: round(v[0] / v[1], 2)) \
#     .sortBy(lambda x: x[1], ascending=False)
# for cell, rate in result.collect():
#     print(f"{cell}: violation_rate = {rate:.0%}")

# Expected output:
# FC-03: violation_rate = 67%   (2 out of 3)
# FC-01: violation_rate = 33%   (1 out of 3)
# FC-02: violation_rate = 33%   (1 out of 3)


spark.stop()
