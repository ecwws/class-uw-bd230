import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object MeetupTrends {

  def main(args: Array[String]) {

    val spark = (SparkSession
      builder()
      appName("Meetup Trends")
      getOrCreate()
    )

    val sc = spark.sparkContext

    val schema = (new StructType()
      add("mtime", LongType)
      add("group",
        new StructType()
          add("group_city", StringType)
          add("group_state", StringType)
          add("group_country", StringType)
          add("group_topics",
            new ArrayType(
              new StructType()
                add("urlkey", StringType)
                add("topic_name", StringType),
              true
            )
          )
      )
    )


    val df = (
      spark.readStream.format("kafka")
      option("kafka.bootstrap.servers", "localhost:29092")
      option("subscribe", "meetup")
      option("startingOffsets", "earliest")
      load()
    )

    val data =
      df.select(col("key").cast("string"),
                from_json(col("value").cast("string"), schema).alias("parsed"))

    val flattened = (
      data.select(
        from_unixtime((col("parsed.mtime")/1000).cast(IntegerType)).cast(TimestampType).as("timestamp"),
        col("parsed.group.group_country").as("country"),
        col("parsed.group.group_city").as("city"),
        explode(col("parsed.group.group_topics")).as("topics")
      )
    )

    val target = (
      flattened
      withWatermark("timestamp", "60 minutes")
      groupBy(
        window(col("timestamp"), "60 minute", "60 minute").as("window"),
        col("country"),
        col("city"),
        col("topics.topic_name").as("topic")
      )
      count()
    )

    val out = (
      target
      select(
        col("window.end").as("timestamp"),
        col("country"),
        col("city"),
        col("topic"),
        col("count")
      )
    )

    val jsonOut = out.selectExpr("CAST(timestamp AS STRING) AS key",
                                    "to_json(struct(*)) AS value")

    val query = (
      jsonOut.writeStream.outputMode("append")
      format("kafka")
      option("kafka.bootstrap.servers", "localhost:29092")
      option("topic", "meetup_trends_1h")
      option("checkpointLocation", "file:///tmp/meetup_trends_1h")
      start
    )

    query.awaitTermination
  }

}

