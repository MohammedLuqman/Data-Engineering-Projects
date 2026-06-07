import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField
from PIL import Image
import pytesseract

spark = SparkSession.builder \
    .appName("Yemen_Archive_OCR_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def perform_arabic_ocr(file_path):
    if not os.path.exists(file_path):
        return "[نص مستخرج تلقائياً]"
    
    try:
        img = Image.open(file_path)
        text = pytesseract.image_to_string(img, lang='ara')
        return text.strip()
    except Exception as e:
        return f"Error: {str(e)}"

ocr_udf = udf(perform_arabic_ocr, StringType())

json_schema = StructType([
    StructField("document_id", StringType(), True),
    StructField("owner_name", StringType(), True),
    StructField("document_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("issue_date", StringType(), True),
    StructField("metadata", StructType([
        StructField("archive_status", StringType(), True),
        StructField("file_path", StringType(), True)
    ]), True)
])

kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("subscribe", "yemen_archive_events") \
    .option("startingOffsets", "latest") \
    .load()

processed_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), json_schema).alias("data")) \
    .select("data.*") \
    .withColumn("extracted_text", ocr_udf(col("metadata.file_path"))) \
    .withColumn("processed_at", current_timestamp())

def write_to_sinks(batch_df, batch_id):
    if batch_df.count() > 0:
        
        # (PostgreSQL)
        postgres_df = batch_df.select(
            "document_id", "owner_name", "document_type", "city", 
            col("issue_date").cast("date"), "extracted_text"
        )
        
        try:
            postgres_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/ye_archive") \
                .option("dbtable", "yemeni_documents") \
                .option("user", "postgres") \
                .option("password", "rootpassword") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            pass
            
        # (Hadoop HDFS)
        hdfs_df = batch_df.select(
            "document_id", "owner_name", "document_type", "city", "issue_date", 
            "extracted_text", col("metadata.file_path").alias("raw_file_path"), "processed_at"
        )
        
        try:
            hdfs_df.write \
                .format("parquet") \
                .mode("append") \
                .save("hdfs://namenode:9000/yemen_archive/historical_documents/")
        except Exception as e:
            pass

query = processed_df.writeStream \
    .foreachBatch(write_to_sinks) \
    .start()

query.awaitTermination()