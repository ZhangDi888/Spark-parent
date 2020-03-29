package com.zhang.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.zhang.gmall0826.common.constat.GmallConstant
import com.zhang.gmall0826.realtime.been.{AlertInfo, EventInfo}
import com.zhang.gmall0826.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    val ssc = new StreamingContext(conf, Seconds(5))
    //获取到kafka消费的数据
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)
//    kafkaDS.map(_.value()).print()
    //对数据进行转换结构
    val kafkaDSMap: DStream[EventInfo] = kafkaDS.map {
      record =>
        val JsonString: String = record.value()
        //将日志数据转成样例类，方便后续操作
        val eventLog: EventInfo = JSON.parseObject(JsonString, classOf[EventInfo])
        val formatDate = new SimpleDateFormat("yyyy-MM-dd HH")
        val date = new Date(eventLog.ts)
        //对日志里的时间进行转换格式
        val dateString: String = formatDate.format(date)
        //按照空格切分，补充到样例类的时间字段中
        val strings: Array[String] = dateString.split(" ")
        //补全字段
        eventLog.logDate = strings(0)
        eventLog.logHour = strings(1)
        eventLog
    }


//        拆
//        1 同一设备   groupbykey
//        2 5分钟内    window
//        3 三次及以上用不同账号登录并领取优惠劵   //  对操作集合进行判断
//          4 并且在登录到领劵过程中没有浏览商品
//
//        5 按照日志的格式进行保存  //调整结构
    val kafkaWindow: DStream[EventInfo] = kafkaDSMap.window(Seconds(300), Seconds(5))
    //先将数据映射成（mid,数据），这样方便后续根据mid进行分组
    val keyMapDS: DStream[(String, EventInfo)] = kafkaWindow.map {
      eventInfo => {
        (eventInfo.mid, eventInfo)
      }
    }
    //将数据根据mid进行分组
    val keyDS: DStream[(String, Iterable[EventInfo])] = keyMapDS.groupByKey()
    //需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
    // 并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志。
    //同一设备，每分钟只记录一次预警。
     val mapDS: DStream[(Boolean, AlertInfo)] = keyDS.map {
       case (mid, iterable) => {
         //HashSet用java的包不是因为scala不好用，而是因为为了和后面es对接方便
         val uidSet = new util.HashSet[String]()
         val eventList = new util.ArrayList[String]()
         val itemIds = new util.HashSet[String]()
         var flag = false;
         breakable(
           for (elem <- iterable) {
             //将一行数据的事件添加到list里
             eventList.add(elem.evid)
             if (elem.evid == "coupon") {
               //如果领券了就添加uid，set可以去重，如果多个不同id就可以统计出来
               uidSet.add(elem.uid)
               //后面需要用到领券的商品
               itemIds.add(elem.itemid)
             }
             //如果领券又点击了商品，就退出循环
             if (elem.evid == "clickItem") {
               flag = true
               break()
             }
           }
         )

         //如果同个设备，领券用户大于三个，同时又没点击过商品
         val ifAlert: Boolean = uidSet.size() >= 3 && !flag
         //如果flag为true，中断循环，然后ifAlert为false，相当于为点击过商品的做个false标签，到时候过滤只留下没点过的
         //AlertInfo,是通过样例类存进es的
         (ifAlert, AlertInfo(mid, uidSet, itemIds, eventList, System.currentTimeMillis()))
       }
     }
  val filterDS: DStream[AlertInfo] = mapDS.filter(_._1).map(_._2)
//    filterDS.print()

    filterDS.foreachRDD{ rdd => {
        rdd.foreachPartition{ AlertlnfoItr => {
            val sourceList: List[(String, AlertInfo)] = AlertlnfoItr.toList.map { AlertInfo => {(AlertInfo.mid + "_" + AlertInfo.ts / 1000 / 60, AlertInfo)}}
            MyEsUtil.insertBulk(sourceList,GmallConstant.ES_INDEX_ALERT)
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
