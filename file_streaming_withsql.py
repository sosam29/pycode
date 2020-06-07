
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    sparkSession= SparkSession.builder.appName("AggregationDemo").getOrCreate()
    
    sparkSession.sparkContext.setLogLevel('ERROR')
    
    schema = StructType([StructField("lsoa_code",StringType(), True),
                         StructField("borough",StringType(), True),
                         StructField("major_category",StringType(), True),
                         StructField("minor_category",StringType(), True),
                         StructField("value",StringType(), True),
                         StructField("year",StringType(), True),
                         StructField("month",StringType(), True)                                                                                        
                         ])
    fileSchemaDF = sparkSession.readStream\
        .option("header","true")\
        .option("maxFilesPerTrigger", 2)\
        .schema(schema)\
        .csv(r"C:\Users\sosam\Downloads\apache-spark-2-structured-streaming\02\demos\datasets\droplocation")
    
    fileSchemaDF.createOrReplaceTempView("LondonCrimeData")
    
    categoryDF= sparkSession.sql("Select major_category, value\
                                 from LondonCrimeData\
                                     where year='2016'")
        
    convictionPerCategory=categoryDF.groupBy("major_category")\
        .agg({"value":"sum"})\
            .withColumnRenamed("sum(value)","convictions")\
                .orderBy("convictions", ascending=False)
    
    query = convictionPerCategory.writeStream \
        .outputMode("complete")\
            .format("console")\
                .option("truncate", "false")\
                    .option("numRows", 30)\
                        .start() \
                            .awaitTermination()
            
    