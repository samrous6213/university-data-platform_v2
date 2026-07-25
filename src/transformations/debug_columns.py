import json, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.transformations.readers.minio_reader import read_json

spark = SparkSession.builder.appName("debug").getOrCreate()
bucket = "raw-json"
prefix = "source=openalex/"

lines = []

df = read_json(spark, bucket, prefix=prefix)
cnt = df.count()
lines.append(f"SOURCE: openalex  rows={cnt}")

if cnt > 0:
    cols = df.columns
    lines.append(f"TOP-LEVEL COLUMNS: {cols}")

    if "results" in cols:
        st = df.schema["results"].simpleString()
        lines.append(f"FIELD: results  type={st}")

        exploded = df.selectExpr("inline_outer(results)")
        expl_cols = exploded.columns
        lines.append(f"EXPLODED (inline_outer) COLUMNS ({len(expl_cols)}): {expl_cols}")

        seen = {}
        dups = []
        for c in expl_cols:
            lower = c.lower()
            if lower in seen:
                dups.append((c, seen[lower]))
            else:
                seen[lower] = c
        if dups:
            lines.append(f"DUPLICATE COLUMNS (case-insensitive):")
            for d in dups:
                lines.append(f"  {d[0]}  <->  {d[1]}")
        else:
            lines.append("NO duplicate column names found.")

        sample = exploded.limit(1).toJSON().first()
        lines.append(f"\nSAMPLE EXPLODED ROW:\n{sample}")
    else:
        lines.append("WARNING: no 'results' field found in openalex data")
        row_json = df.limit(1).toJSON().first()
        lines.append(f"SAMPLE RAW: {row_json[:2000]}")
else:
    lines.append("WARNING: no openalex data found")

out = "\n".join(lines)
print(out)

with open("/tmp/debug_out.txt", "w") as f:
    f.write(out)

spark.stop()
