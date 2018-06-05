import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr

object MeetupTrends {

  def main(args: Array[String]) {

    val dst = args(0)

    val spark = (SparkSession
      builder()
      appName("Meetup Trends")
      getOrCreate()
    )

    val meetup = (new StructType()
      add("mtime", LongType)
      add("rsvp_id", LongType)
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

    val weather = (new StructType()
      add("currently", new StructType()
        add("time", LongType)
        add("temperature", FloatType)
        add("humidity", FloatType)
        add("pressure", FloatType)
        add("windSpeed", FloatType)
      )
      add("meetup_rsvp_id", LongType)
    )


    val meetup_stream = (
      spark.readStream.format("kafka")
      option("kafka.bootstrap.servers", "localhost:29092")
      option("subscribe", "meetup")
      option("startingOffsets", "earliest")
      load()
    )

    val weather_stream = (
      spark.readStream.format("kafka")
      option("kafka.bootstrap.servers", "localhost:29092")
      option("subscribe", "meetup_weather_ref")
      option("startingOffsets", "earliest")
      load()
    )



    val meetup_data =
      meetup_stream.select(col("key").cast("string"),
        from_json(col("value").cast("string"), meetup).alias("parsed"))

    val weather_data =
      weather_stream.select(col("key").cast("string"),
        from_json(col("value").cast("string"), weather).alias("parsed"))


    val flattened = (
      meetup_data
      filter(col("parsed.group.group_country") === "us")
      select(
        from_unixtime((col("parsed.mtime")/1000).cast(IntegerType)).cast(TimestampType).as("timestamp"),
        col("parsed.group.group_country").as("country"),
        col("parsed.group.group_city").as("city"),
        col("parsed.rsvp_id").as("rsvp_id"),
        explode(col("parsed.group.group_topics")).as("topics")
      )
      withWatermark("timestamp", "5 minutes")
    )

    val weather_parsed = (
      weather_data.select(
        from_unixtime(col("parsed.currently.time")).cast(TimestampType).as("weather_timestamp"),
        col("parsed.meetup_rsvp_id").as("meetup_rsvp_id"),
        col("parsed.currently.temperature").as("temperature"),
        col("parsed.currently.pressure").as("pressure"),
        col("parsed.currently.windSpeed").as("windSpeed"),
        col("parsed.currently.humidity").as("humidity")
      )
      withWatermark("weather_timestamp", "5 minutes")
    )

    val joined =
      flattened.join(weather_parsed, expr("rsvp_id = meetup_rsvp_id"))

    val target = (
      joined
      groupBy(
        window(col("timestamp"), "5 minute", "5 minute").as("window"),
        col("country"),
        col("city"),
        col("topics.topic_name").as("topic")
      )
      agg(
        avg("temperature").as("avg_tmp"),
        avg("pressure").as("avg_pres"),
        avg("windSpeed").as("avg_wind"),
        avg("humidity").as("avg_humi")
      )
    )

    val out = (
      target
      select(
        col("window.end").as("timestamp"),
        col("country"),
        col("city"),
        col("topic"),
        col("avg_tmp")
      )
    )

    val jsonOut = out.selectExpr("CAST(timestamp AS STRING) AS key",
                                    "to_json(struct(*)) AS value")

    val query = (
      jsonOut.writeStream.outputMode("append")
      format("kafka")
      option("kafka.bootstrap.servers", "localhost:29092")
      option("topic", dst)
      option("checkpointLocation", "file:///tmp/" + dst)
      start
    )

    query.awaitTermination
  }

}

