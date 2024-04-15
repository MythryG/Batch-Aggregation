from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg
import sys

def main(input_file, output_dir):
    # Initializing the Spark session
    spark = SparkSession.builder.appName("BatchAggregationCode").getOrCreate()

    # Reading the input data
    try:
        input_data = spark.read.csv(input_file, header=True, inferSchema=True)
    except Exception as e:
        print(f"Error reading input file: {e}")
        spark.stop()
        sys.exit(1)

    # Defining the time bucket duration
    time_bucket_duration = "1 day"

    # Aggregating the data
    try:
        aggregated_data = input_data.groupBy(
            "metric",
            window("timestamp", time_bucket_duration)
        ).agg(avg("value").alias("average_value"))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        spark.stop()
        sys.exit(1)

    # Writing the output data
    try:
        aggregated_data.write.csv(output_dir, header=True)
    except Exception as e:
        print(f"Error writing output data: {e}")
    
    # Stoping the Spark session
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: batch_aggregation.py <input_file> <output_dir>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
