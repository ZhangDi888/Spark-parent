package com.zhang.gmall0826.realtime.app


import com.alibaba.fastjson.JSON
import com.zhang.gmall0826.common.constat.GmallConstant
import com.zhang.gmall0826.realtime.been.OrderInfo
import com.zhang.gmall0826.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {
/*
* 统计订单
* */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Order_info").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //获取到order，topic的数据
    val KafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
    //对数据进行转换结构
    val orderLog: DStream[OrderInfo] = KafkaDS.map {
      record =>
        //获取到数据
        val KafkaValue: String = record.value()
        //把数据转换成样例类格式
        val orderLog: OrderInfo = JSON.parseObject(KafkaValue, classOf[OrderInfo])
        //转换日期格式
        // val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val strings: Array[String] = orderLog.create_time.split(" ")
        //对时间进行切分之后，对属性进行赋值
        orderLog.create_date = strings(0)
        orderLog.create_hour = strings(1).split(":")(0)
        //进行脱敏工作
        //对手机号从第3个开始切，切成两半
        val tuple: (String, String) = orderLog.consignee_tel.splitAt(3)
        //获得手机号前3位
        val take3: String = tuple._1
        //获得手机号后四位
        val right4: String = tuple._2.splitAt(4)._2
        orderLog.consignee_tel = take3 + "****" + right4
        orderLog
    }
    orderLog.foreachRDD{
      rdd=>rdd.saveToPhoenix("GMALL0826_ORDER_INFO",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL",
        "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL",
        "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO",
        "PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }
    ssc.start()
    ssc.awaitTermination()
  }
  }
