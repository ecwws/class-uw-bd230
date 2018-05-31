import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._
import org.apache.spark.SparkConf

object MeetupTrends {

  def main(args: Array[String]) {

    val spark = (SparkSession
      builder()
      appName("Meetup Analysis")
      config("es.index.auto.create", "true")
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
      load()
    )

    val data =
      df.select(col("key").cast("string"),
                from_json(col("value").cast("string"), schema).alias("parsed"))

    val flattened = (
      data.select(
        from_unixtime((col("parsed.mtime")/1000).cast(IntegerType)).as("timestamp"),
        col("parsed.group.group_country").as("country"),
        col("parsed.group.group_city").as("city"),
        explode(col("parsed.group.group_topics")).as("topics")
      )
    )

    val target = (
      flattened
      withWatermark("timestamp", "2 minutes")
      groupBy(
        window(col("timestamp"), "2 minute", "1 minute").as("window"),
        col("country"),
        col("city"),
        col("topics.topic_name").as("topic")
      )
      count()
    )

    val query = (target.writeStream.outputMode("append")
      format("es")
      option("checkpointLocation", "/tmp/es")
      start("meetup/trends")
    )
    // val jsonOut = target.selectExpr("CAST(window AS STRING) AS key",
    //                                 "to_json(struct(*)) AS value")
    //
    // val query = (
    //   jsonOut.writeStream.outputMode("update")
    //   format("kafka")
    //   option("kafka.bootstrap.servers", "localhost:29092")
    //   option("topic", "meetup_analysis")
    //   option("checkpointLocation", "file:///tmp/kafka")
    //   start
    // )
    //
    query.awaitTermination
  }

}

