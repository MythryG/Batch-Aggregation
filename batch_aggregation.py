from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, min, max
from pyspark.sql import functions as F
import sys

def main(input_file, output_dir, time_bucket_duration):
    # Initializing the Spark session
    spark = SparkSession.builder.appName("BatchAggregationCode").getOrCreate()

    # Reading the input data
    try:
        input_data = spark.read.csv(input_file, header=True, inferSchema=True)
    except Exception as e:
        print(f"Error reading input file: {e}")
        spark.stop()
        sys.exit(1)

    # Aggregating the data
    try:
        aggregated_data = input_data.groupBy("metric",window("timestamp", time_bucket_duration)
        ).agg(avg("value").alias("average_value"),min("value").alias("minimum_value"),max("value").alias("maximum_value"))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        spark.stop()
        sys.exit(1)

    # Writing the output data
    try:
        final_aggregated_data=aggregated_data.withColumn('time_bucket', F.to_json('window')).drop("window")
        aggregated_data.write.csv(output_dir, header=True)
    except Exception as e:
        print(f"Error writing output data: {e}")
    
    # Stoping the Spark session
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: batch_aggregation.py <input_file> <output_dir> <time_bucket_duration>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3])
