package com.atguigu.app

import java.text.SimpleDateFormat
import java.{lang, util}
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.startUpLog
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.utils.{MykafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._


/**
 * @author zbstart
 * @create 2020-10-16 11:24
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费kafka
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.数据流 转换 结构变成case class 补充两个时间字段
    val startuplogDstream: DStream[startUpLog] = inputDstream.map { record =>
      val jsonStr: String = record.value()
      val startupLog: startUpLog = JSON.parseObject(jsonStr, classOf[startUpLog])

      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
      val dateArr: Array[String] = dateTimeStr.split(" ")
      startupLog.logDate = dateArr(0)
      startupLog.logHour = dateArr(1)
      startupLog
    }
    //    startuplogDstream.print()
    startuplogDstream.cache()

    //5.跨批次去重
    val filterByRedisLogDStream: DStream[startUpLog] = DauHandler.filterByRedis(startuplogDstream, ssc.sparkContext)
    filterByRedisLogDStream.cache()
    filterByRedisLogDStream.count().print()

    //6.同批次去重
    val filterByMidLogDStream: DStream[startUpLog] = DauHandler.filterByMid(filterByRedisLogDStream)
    filterByMidLogDStream.cache()

    //7.将去重之后的数据中的mid写入redis
    DauHandler.saveMidToRedis(filterByMidLogDStream)

    //8.將去重之后的数据明细写入phoenix
    filterByMidLogDStream.foreachRDD { rdd =>
      rdd.saveToPhoenix("GMALL2020_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))

      ssc.start()
      ssc.awaitTermination()
    }
  }

}
