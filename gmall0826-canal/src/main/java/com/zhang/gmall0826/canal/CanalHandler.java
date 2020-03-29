package com.zhang.gmall0826.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zhang.gmall0826.canal.util.MyKafkaSender;
import com.zhang.gmall0826.common.constat.GmallConstant;

import java.util.List;

public class CanalHandler {

    List<CanalEntry.RowData> rowDatasList;

    CanalEntry.EventType eventType;

    String tableName;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDatasList = rowDataList;
    }

    public void handle() {
        //如果rowlist对象不为空，或者对象里面个数大于0
        if (this.rowDatasList != null && this.rowDatasList.size() > 0) {
            //如果表是order_info并且事件类型是insert
            if (tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT) {
                sendKafka(rowDatasList,GmallConstant.KAFKA_TOPIC_ORDER); //下单
            } else if (tableName.equals("order_detail") && eventType == CanalEntry.EventType.INSERT){
                sendKafka(rowDatasList,GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);//下单明细
            } else if (tableName.equals("user_info") && eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE){
                sendKafka(rowDatasList,GmallConstant.KAFKA_TOPIC_USER);//用户
            }
        }
    }

    public void sendKafka(List<CanalEntry.RowData> rowDataList, String topic) {
        //对row集合进行迭代
        for (CanalEntry.RowData rowData : rowDataList) {
            //因为下订单是数据操作之后产生的，所以选择after，如果是删除业务，就选择getBefore
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            //rowData类型的数据不适合发送，转成jsonString的在发送
            JSONObject jsonObject = new JSONObject();
            //在对这个订单进行迭代，输出列名和值，放到json里面就组成了一行数据
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "::" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }
            //通过kafka工具类，发送数据到指定的topic
            MyKafkaSender.send(topic, jsonObject.toJSONString());
        }
    }
}
