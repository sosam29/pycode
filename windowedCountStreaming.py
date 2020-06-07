from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import time
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import window


if __name__=="__main__":
    
    spark= SparkSession.builder.appName("Windowedcount").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    schema= StructType([StructField("lsoa_code",StringType(), True),
                        StructField("borough",StringType(), True),
                        StructField("major_category",StringType(), True),
                        StructField("minor_category",StringType(), True),
                        StructField("value",StringType(), True),
                        StructField("year",StringType(), True),
                        StructField("month",StringType(), True)])
    
    fileStreamDF= spark.readStream\
        .option("header", "true")\
            .option("maxFilesPreTrigger", 2)\
                .schema(schema)\
                    .csv(r"C:\Users\sosam\Downloads\apache-spark-2-structured-streaming\02\demos\datasets\droplocation")
                    
    
    def add_time_stamp():
        ts= time.time()
        timestamp= datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return timestamp
        
    add_time_stamp_udf= udf(add_time_stamp, StringType())
    
    fileStreamWithTS= fileStreamDF.withColumn("timestamp", add_time_stamp_udf())
    
    windowedCounts= fileStreamWithTS.groupBy(
        window(fileStreamWithTS.timestamp,
               "30 seconds",
               "18 seconds"))\
               .agg({"value":"sum"})\
                   .withColumnRenamed("sum(value)","convictions")\
                       .orderBy("convictions", asecending=False)
                       
    query = windowedCounts.writeStream\
        .outputMode("complete")\
            .format("console")\
                .option("truncate", "false")\
                    .start()\
                        .awaitTermination()
                        
               


