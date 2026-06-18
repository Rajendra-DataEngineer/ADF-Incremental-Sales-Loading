# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Sales Load — PySpark Delta Lake MERGE
# MAGIC
# MAGIC Converts the ADF/SQL watermark + upsert pattern to a native Delta Lake MERGE.
# MAGIC Equivalent to `01_Incremental_sales_pipeline` but runs on Databricks.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# Configuration — update paths for your environment
SOURCE_PATH = "abfss://input@rndlearning.dfs.core.windows.net/salesdata1.csv"
TARGET_TABLE = "main.sales.sales_target"
WATERMARK_TABLE = "main.sales.watermark_table"
TABLE_NAME = "Sales_Target"

# COMMAND ----------

def get_watermark(spark, watermark_table: str, table_name: str) -> str:
    """Read last watermark; default to 1900-01-01 on first run."""
    if not spark.catalog.tableExists(watermark_table):
        return "1900-01-01"

    row = (
        spark.table(watermark_table)
        .filter(F.col("TableName") == table_name)
        .select("WatermarkValue")
        .collect()
    )
    if not row:
        return "1900-01-01"
    return str(row[0][0])[:10]


def read_source_csv(spark, source_path: str):
    """Read CSV with schema drift tolerance."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(source_path)
        .select(
            F.col("saleID").cast("int").alias("SaleID"),
            F.col("Product").cast("string"),
            F.col("amount").cast("decimal(10,2)").alias("Amount"),
            F.to_timestamp(F.col("saledate"), "yyyy-MM-dd").alias("LastModifiedDate"),
        )
    )


def merge_incremental(spark, new_data, target_table: str):
    """Delta MERGE: upsert on SaleID, update only when source is newer."""
    if not spark.catalog.tableExists(target_table):
        new_data.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(
            new_data.alias("s"),
            "t.SaleID = s.SaleID",
        )
        .whenMatchedUpdate(
            condition="s.LastModifiedDate > t.LastModifiedDate",
            set={
                "Product": "s.Product",
                "Amount": "s.Amount",
                "LastModifiedDate": "s.LastModifiedDate",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def update_watermark(spark, watermark_table: str, table_name: str, new_value: str):
    """Persist watermark after successful MERGE."""
    schema = StructType([
        StructField("TableName", StringType(), False),
        StructField("WatermarkValue", StringType(), False),
    ])
    watermark_df = spark.createDataFrame(
        [(table_name, new_value)], schema=schema
    )

    if not spark.catalog.tableExists(watermark_table):
        watermark_df.write.format("delta").mode("overwrite").saveAsTable(watermark_table)
        return

    wm = DeltaTable.forName(spark, watermark_table)
    (
        wm.alias("t")
        .merge(watermark_df.alias("s"), "t.TableName = s.TableName")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# Main execution
watermark = get_watermark(spark, WATERMARK_TABLE, TABLE_NAME)
print(f"Current watermark: {watermark}")

source_df = read_source_csv(spark, SOURCE_PATH)
new_data = source_df.filter(F.col("LastModifiedDate") > F.lit(watermark))

record_count = new_data.count()
print(f"New/changed records to merge: {record_count}")

if record_count > 0:
    merge_incremental(spark, new_data, TARGET_TABLE)

    new_watermark = (
        spark.table(TARGET_TABLE)
        .agg(F.max("LastModifiedDate").alias("max_date"))
        .collect()[0]["max_date"]
    )
    update_watermark(spark, WATERMARK_TABLE, TABLE_NAME, str(new_watermark)[:10])
    print(f"Watermark updated to: {new_watermark}")
else:
    print("No new records — watermark unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Run after MERGE to confirm results.

# COMMAND ----------

display(spark.table(TARGET_TABLE).orderBy(F.desc("LastModifiedDate")).limit(10))

if spark.catalog.tableExists(WATERMARK_TABLE):
    display(spark.table(WATERMARK_TABLE))
