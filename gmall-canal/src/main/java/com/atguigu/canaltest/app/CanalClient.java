package com.atguigu.canaltest.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.canaltest.utils.MyKafkaSender;
import com.atguigu.gmall.constant.GmallConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author zbstart
 * @create 2020-10-21 11:25
 */
public class CanalClient {

    public static void main(String[] args) {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall2020.*");

            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {
                System.out.println("没有数据，休息一会！！！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        try {
                            //1.获取表名
                            String tableName = entry.getHeader().getTableName();
                            //2.获取数据
                            ByteString storeValue = entry.getStoreValue();
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                            //3.获取数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //4.获取数据操作类型
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            //5.根据不同的表,处理数据
                            handler(tableName, eventType, rowDatasList);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }



                }
            }
        }
    }

    public static void handler(String tabelName,CanalEntry.EventType eventType,List<CanalEntry.RowData> rowDatasList){

        //订单表，只需要新增数据
        if ("order_info".equals(tabelName) && CanalEntry.EventType.INSERT.equals(eventType)){
            for (CanalEntry.RowData rowData: rowDatasList){

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column: rowData.getAfterColumnsList()){
                    jsonObject.put(column.getName(),column.getValue());
                }

                //打印数据，并将数据发送kafka
                System.out.println(jsonObject);
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER_INFO,jsonObject.toString());
            }

        }

    }
}

