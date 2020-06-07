from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import sys
from pyspark.sql.functions import udf
from pyspark.sql.types import *

if __name__ =="__main__":
    
    if len(sys.argv) != 3:
        print("Usage: python countHashTags.py <host> <port> ", file=sys.stderr)
        exit(-1)
        
    host= sys.argv[1]
    port= int(sys.argv[2])

    spark = SparkSession.builder.appName("hashtagcounter").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    lines= spark.readStream.format("socket").option("host", host).option("port", port).load()
    
    words= lines.select(
        explode(
            split(lines.value, " ")
            ).alias("word"))
    
    
    def extract_tags(word):
        if(word.lower().startswith("#")):
            return word
        else:
            return "noTag"
        
    extract_tags_udf = udf(extract_tags, StringType())
    
    resultDF= words.withColumn("tags", extract_tags_udf(words.word))
    
    hashtagCounts= resultDF.where(resultDF.tags != "noTag")\
        .groupBy("tags")\
            .count()\
                .orderBy("count", ascending=False)
                
    query = hashtagCounts.writeStream \
        .outputMode("complete")\
            .format("console")\
                .option("truncate", "false")\
                    .start()\
                        .awaitTermination()
                        