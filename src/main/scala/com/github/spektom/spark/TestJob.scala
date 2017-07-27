package com.github.spektom.spark

import java.net.URI

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.http.HttpInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HTTP Streaming Test")
      // Use local 10 vCPUs
      .master("local[10]")
      // Enable backpressure:
      .config("spark.streaming.backpressure.enabled", "true")
      // Set minimum rate that can be reached during backpressure:
      .config("spark.streaming.backpressure.pid.minRate", "1")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    // Generate 100 URLs
    val urls = (1 to 100).map(n => new URI(s"http://localhost:8000/?${n}"))

    val stream = HttpInputDStream.create(ssc, urls)
    stream.map(s => {
      // This is processing :)
      // Introduce some sleep to emulate situation when processing doesn't keep up:
      Thread.sleep(500)
      s
    }).saveAsTextFiles("/tmp/output/batch")

    ssc.start()
    ssc.awaitTermination()
  }
}
