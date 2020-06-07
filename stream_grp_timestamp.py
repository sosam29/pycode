from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import time
import datetime

if __name__=="__main__":
    spark = SparkSession.builder.appName("GroupByTime").getOrCreate()
    
    spark.sparkContext.setLogLevel('ERROR')
    
    schema= StructType([
        StructField("lsoa_code", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("major_category", StringType(), True),
        StructField("minor_category", StringType(), True),
        StructField("value", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True)
        ])
    
    fileStreamDF= spark.readStream \
        .option("header","true")\
            .option("maxFilesPerTrigger",2)\
                .schema(schema)\
                    .csv(r"C:\Users\sosam\Downloads\apache-spark-2-structured-streaming\02\demos\datasets\droplocation")
    
    def add_timestamp():
        ts = time.time()
        timestamp= datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return timestamp
    
    time_stamp_udf= udf(add_timestamp, StringType())
    
    fileStreamWithTS= fileStreamDF.withColumn("timestamp", time_stamp_udf())
    
    convictionsPerTimeStamp= fileStreamWithTS.groupBy("timestamp")\
        .agg({"value":"sum"})\
            .withColumnRenamed("sum(value)", "convictions")\
                .orderBy("convictions", ascending=False)
    
    
    query= convictionsPerTimeStamp.writeStream\
        .outputMode("complete")\
            .format("console")\
                .option("truncate","false")\
                    .trigger(processingTime="5 seconds")\
                        .start()\
                            .awaitTermination()
                    
    