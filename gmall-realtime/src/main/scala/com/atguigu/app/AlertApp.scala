package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.EventLog
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.utils.MykafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zbstart    
 * @create 2020-10-17 9:00  
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app_log")
    val ssc: StreamingContext = new StreamingContext(sparkconf, Seconds(10))
    val kafkaInputStram: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //创建kafka数据流
    val eventInfoDstream: DStream[EventLog] = kafkaInputStram.map(
      record => {
        //获取value字符串值
        val jsonEventLog: String = record.value()
        val eventlog: EventLog = JSON.parseObject(jsonEventLog, classOf[EventLog])
        eventlog
      }
    )

    //2.开窗口
    val eventInfoWindowDstream: DStream[EventLog] = eventInfoDstream.window(Seconds(30), Seconds(5))

    //3.同一设备 分组
    val groupByMidDstream: DStream[(String, EventLog)] = eventInfoWindowDstream.map(eventlog => {
      (eventlog.mid, eventlog)
    })
    //4 判断预警
//    groupByMidDstream.map()



  }

}
