package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.startUpLog
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.utils.MykafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.phoenix.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @author zbstart
 * @create 2020-10-15 23:05
 */

object RealtimeStartupApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    //创建kafka消费流
    val startupStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(
      GmallConstants.KAFKA_TOPIC_STARTUP,
      ssc
    )

    //       startupStream.map(_.value()).foreachRDD{ rdd=>
    //         println(rdd.collect().mkString("\n"))
    //       }

    val startupLogDstream: DStream[startUpLog] = startupStream.map(_.value()).map { log =>
      // println(s"log = ${log}")
      val startUpLog: startUpLog = JSON.parseObject(log, classOf[startUpLog])
      startUpLog
    }
  }

}