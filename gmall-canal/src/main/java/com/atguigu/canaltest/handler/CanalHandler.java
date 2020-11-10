package com.atguigu.canaltest.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * @author zbstart
 * @create 2020-10-21 14:31
 */

//public class CanalHandler {
//
//    public static void handle(){
//        //下单操作
//        if("order_info".equals(tableName)&& CanalEntry.EventType.INSERT==eventType){
//            rowDateList2Kafka( GmallConstant.KAFKA_TOPIC_ORDER);
//        }else if ("user_info".equals(tableName)&& (CanalEntry.EventType.INSERT==eventType||CanalEntry.EventType.UPDATE==eventType)) {
//            rowDateList2Kafka( GmallConstant.KAFKA_TOPIC_USER);
//        }
//
//    }
//
//    private void  rowDateList2Kafka(String kafkaTopic){
//        for (CanalEntry.RowData rowData : rowDataList) {
//            List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
//            JSONObject jsonObject = new JSONObject();
//            for (CanalEntry.Column column : columnsList) {
//                System.out.println(column.getName()+"::::"+column.getValue());
//                jsonObject.put(column.getName(),column.getValue());
//            }
//
//            MyKafkaSender.send(kafkaTopic,jsonObject.toJSONString());
//        }
//
//    }
//}
