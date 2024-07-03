import os
import io
import time
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.microsoft.azure:azure-storage:8.6.6,org.apache.hadoop:hadoop-azure:3.2.0 pyspark-shell'

# Azure Blob Storage account details
account_name = 'inventorypredictionsa'
account_key = 'W8VRVG7SnwNMvLJVNTPMhCQOA4xGK6CAtbBLlKrZ5nqBeQLA/gZEZnpP1w7lQXxQ4E3qqbuTEyF0+ASt6JXK0w=='
 
container_name = 'raw-parquet-data-cnt'
 
# Set Spark configuration to access Azure Blob Storage
spark = SparkSession.builder.appName("Kafka") \
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .config(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key) \
    .getOrCreate()
 
# Kafka topic to subscribe to
kafka_topic_name = "inventory-dataset"
kafka_bootstrap_servers = "localhost:9092"

blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
container_client = blob_service_client.get_container_client(container_name)

def save_to_buffer(df, epoch_id):
    # Create an in-memory buffer
    buffer = io.BytesIO()
    # Write DataFrame to the buffer in Parquet format
    df.toPandas().to_parquet(buffer, engine='pyarrow')
    buffer.seek(0)

    # Generate a filename with a timestamp
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = f"{timestamp}.parquet"

    # Upload the buffer content to Azure Blob Storage
    blob_client = container_client.get_blob_client(filename)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f"Uploaded {filename} to Azure Blob Storage")

def main():

    #Schema
    Schema = StructType([
    StructField("InvoiceNo", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", StringType()),
    StructField("InvoiceDate", StringType()),
    StructField("UnitPrice", StringType()),
    StructField("CustomerID", StringType()),
    StructField("Country", StringType()),
    StructField("Promotion Code", StringType()),
    StructField("Employee ID", StringType())
    ])
 
    def read_kafka_topic(topic, schema):
        return(spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', 'localhost:9092')
        .option('subscribe', topic)
        .option('startingOffsets','earliest')
        .load()
        .selectExpr('CAST(value AS STRING)')
        .select(from_json(col('value'), schema).alias('data'))
        .select('data.*')
    )
    
    data = read_kafka_topic(kafka_topic_name, Schema).alias('project_data')

    def streamWriter (input: DataFrame):
        return (input.writeStream
        .foreachBatch(save_to_buffer)
        .outputMode('append')
        .trigger(processingTime='15 minutes')
        .start())

    query = streamWriter(data)

    query.awaitTermination(7200)

if __name__ == "__main__":
    main()