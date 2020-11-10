package com.atguigu.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.startUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author zbstart    
 * @create 2020-10-20 15:17  
 */
object DauHandler {

  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-dd")

  //同批次去重
  def filterByMid(filterByRedisLogDStream: DStream[startUpLog]): DStream[startUpLog] = {

    // 将数据转换为元组 ==>(mid,startUpLog)
    val midDateToLogDStream: DStream[(String, startUpLog)] = filterByRedisLogDStream.map(startUpLog => {
      (s"${startUpLog.mid}-${startUpLog.logDate}", startUpLog)
    })
    //按照mid分组
    val midDateToLogDStreamIter: DStream[(String, Iterable[startUpLog])] = midDateToLogDStream.groupByKey()

    midDateToLogDStreamIter.flatMap {
      case (_,iter) =>
        iter.toList.sortWith(_.ts < _.ts).take(1)
    }

  }


  //跨批次去重
  def filterByRedis(startUpLogDStream: DStream[startUpLog],sc: SparkContext)={
    startUpLogDStream.filter(startUpLog=>{
      //获取jedis连接
      val jedis: Jedis = RedisUtil.getJedisClient
      //判断redis是否存在mid
      val exist: lang.Boolean = jedis.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid)
      //释放连接
      jedis.close()
      //判断是否存在
      !exist
    })
  }

  def saveMidToRedis(startUpLogDstream: DStream[startUpLog]):Unit = {
    //方案一
    startUpLogDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(
        iter=>{
          val jedis: Jedis = RedisUtil.getJedisClient
          iter.foreach(startUpLog=>{
            val redisKey: String = s"DAU:${startUpLog.logDate}"
            jedis.sadd(redisKey,startUpLog.mid)
          }
          )
          jedis.close()
        }
      )
    })
    //方案三
//    startUpLogDstream.transform(rdd=>{
//      //获取redis连接
//      val jedis: Jedis = RedisUtil.getJedisClient
//      //查询redis当日所有登陆过的mid
//      val midSet: util.Set[String] = jedis.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
//      //广播
//      val midSetBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)
//      //释放连接
//      jedis.close()
//      //在excutor端进行过滤操作
//      rdd.filter(startUpLog=>{
//        midSetBC.value.contains()
//      })
//
//      rdd
//    })




  }
}
