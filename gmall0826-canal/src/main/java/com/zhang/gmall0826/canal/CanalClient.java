package com.zhang.gmall0826.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {
        //1.连接到canal的server端
        //CanalConnectors,带s的一般都是工具类，如果canal不是集群，就选Sing
        //如果有多个example去监控sql，就要创建多个客户端的类
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {//常驻内存
            //获取连接
            canalConnector.connect();
            //2.抓取数据,*.*代表所以数据库的所有表
            canalConnector.subscribe("*.*");
            //从队列抓取数据个数
            //100代表100个sql单元，1个单元，指一句sql执行后影响了多少row的集合
            Message message = canalConnector.get(100);
            if (message.getEntries().size() == 0) {
                System.out.println("没有数据，休息一会~");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //3.把抓取的数据展开，转成想要的格式
                for (CanalEntry.Entry entry : message.getEntries()) {//有数据，entry == sql单位
                    //只要rowdate类型的数据
                    if(CanalEntry.EntryType.ROWDATA == entry.getEntryType()){
                        //获取sql的压缩文件
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //需要解压或者称为反序列化
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //得到行集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //得到操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //得到表名
                        String tableName = entry.getHeader().getTableName();
                        //表名和操作类型确定topic的类型，行集合确定topic内容

                        //4.根据不同的业务发送到kafka不同的topic
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList); //只有给我这三个数据，才能知道数据怎么处理
                        canalHandler.handle();

                    }

                }
            }
        }


    }
}
