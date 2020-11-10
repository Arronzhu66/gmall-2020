package com.atguigu.bean

/**
 * @author zbstart    
 * @create 2020-10-17 9:00  
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
