package org.apache.spark.streaming.http

import java.net.URI

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * Direct HTTP input stream that produces RDD[String]
  */
class HttpInputDStream(ssc_ : StreamingContext, urls: Seq[URI]) extends InputDStream[String](ssc_) {

  var prevRate: Long = -1

  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new HttpRateController(id,
        RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }

  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def compute(validTime: Time): Option[RDD[String]] = {
    val newRate = rateController.get.getLatestRate()
    val skipBatch = newRate != -1 && newRate < prevRate
    prevRate = newRate
    val rdd = new HttpRDD(ssc_.sparkContext, urls, skipBatch)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, StreamInputInfo(id, rdd.count))
    Some(rdd)
  }

  private[streaming] class HttpRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }
}

class HttpPartition(val idx: Int) extends Partition {
  override def index: Int = idx
}

class HttpRDD(@transient val sc: SparkContext, urls: Seq[URI], skipBatch: Boolean) extends RDD[String](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    if (skipBatch) {
      // We've been told to reduce the rate, so let's skip this task, and return an empty RDD:
      Iterator()
    } else {
      val httpClient = HttpClients.createDefault()
      try {
        val httpGet = new HttpGet(urls(split.index))
        Iterator(EntityUtils.toString(httpClient.execute(httpGet).getEntity))
      } finally {
        httpClient.close()
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    urls.zipWithIndex.map { case (_, idx) => new HttpPartition(idx) }.toArray
  }
}

object HttpInputDStream {
  def create(ssc: StreamingContext, urs: Seq[URI]): InputDStream[String] = {
    new HttpInputDStream(ssc, urs)
  }
}