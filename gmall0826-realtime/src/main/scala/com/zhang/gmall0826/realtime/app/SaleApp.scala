package com.zhang.gmall0826.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.zhang.gmall0826.common.constat.GmallConstant
import com.zhang.gmall0826.realtime.been.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.zhang.gmall0826.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //获取order和orderDetail的消费kafka的数据
    val orderDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
    val orderDetailDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    //对数据进行结构的转换以及脱敏，record类型不好使，需要先转成json，在操作
    val orderMap: DStream[OrderInfo] = orderDS.map { record => {
      val kfkaValue: String = record.value()
      val jesonString: OrderInfo = JSON.parseObject(kfkaValue, classOf[OrderInfo])
      val strings: Array[String] = jesonString.create_time.split(" ")
      jesonString.create_date = strings(0)
      jesonString.create_hour = strings(1).split(":")(0)
      val tuple: (String, String) = jesonString.consignee_tel.splitAt(3)
      jesonString.consignee_tel = tuple._1 + "*****" + tuple._2.splitAt(4)._2
      jesonString
    }
    }
    //对数据进行结构的转换以及脱敏（这里步骤和上边类似）
    val orderDetailMap: DStream[OrderDetail] = orderDetailDS.map(record => {
      val str: String = record.value()
      val detail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])
      detail
    })
    //首先将两个表以id+表数据映射出来，然后根据两个表的id进行全外联
    val orderMapByKey: DStream[(String, OrderInfo)] = orderMap.map(order => (order.id, order))
    val orderDetailMapBykey: DStream[(String, OrderDetail)] = orderDetailMap.map(orderDetail => (orderDetail.order_id, orderDetail))

    val orderFullJoinOrderdetail: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderMapByKey.fullOuterJoin(orderDetailMapBykey)
    val saleDetailDS: DStream[SaleDetail] = orderFullJoinOrderdetail.flatMap { case (id, (orderInfoOption, orderDetailOption)) => {
      //1.主表部分
      //1.1在同一批次能够关联，两个对象组合成新的宽表
      val listBuffer = new ListBuffer[SaleDetail]
      //获取到redis的客户端
      val jedis: Jedis = RedisUtil.getJedisClient
      if (orderInfoOption != None) {
        //如果这个值不为none，取出来
        val orderInfo: OrderInfo = orderInfoOption.get
        if (orderDetailOption != None) {
          //如果从表的值不为none
          val orderDetail: OrderDetail = orderDetailOption.get
          //两个表以该id关联的数据合并到宽表中
          val detail = new SaleDetail(orderInfo, orderDetail)
          //提交到list集合中
          listBuffer += detail
        }
        // Redis ？  type ?   set     key ?   order_detail:[order_id]       value ? orderDetailJsons
        //1.2转换成json写入缓存
        val orderInfoJson: String = JSON.toJSONString(orderInfo, new SerializeConfig(true))
        val orderByKey: String = "order_info:" + orderInfo.id
        jedis.setex(orderByKey, 600, orderInfoJson)
        //1.3查缓存是否有对应的orderDetail
        val orderDetailByKey: String = "order_detail:" + orderInfo.id
        val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailByKey)
        if (orderDetailJsonSet != null && orderDetailJsonSet.size() > 0) {
          //因为json是java的，要想在scala中使用需要导包
          import scala.collection.JavaConversions._
          //因为订单明细是一对多的关系，所以需要先遍历
          for (orderDetailString <- orderDetailJsonSet) {
            //先转换成样例类
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            listBuffer += saleDetail
          }
        }
      } else {
        //2.从表 ,默认从表有数据
        //先获取到数据，缓存到redis
        val orderDetail: OrderDetail = orderDetailOption.get
        //先转成json
        val orderDetailJson: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
        val orderDetailByKey: String = "order_detail:" + orderDetail.order_id
        jedis.sadd(orderDetailByKey, orderDetailJson)
        jedis.expire(orderDetailByKey, 600)
        //            jedis.setex(orderDetailByKey, 600, orderDetailJson)

        //2.2从缓存中查找
        val orderInfoKey: String = "order_info:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.length > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          listBuffer += new SaleDetail(orderInfo, orderDetail)
        }
      }
      jedis.close()
      listBuffer
    }
    }
//0.存量用户,将用户写一个批处理程序,写入到redis
    val userDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER,ssc)
    userDS.foreachRDD(rdd=>{
      val userRDD: RDD[String] = rdd.map(_.value())
      userRDD.foreachPartition{
        Iterator => {
          val jedis: Jedis = RedisUtil.getJedisClient
          for (userJsonString <- Iterator) {
            //存redis type? string key? user_info:[user_id] value?userJsonString
            val userInfo: UserInfo = JSON.parseObject(userJsonString,classOf[UserInfo])
            val userKey: String = "user_info:" + userInfo.id
            jedis.set(userKey,userJsonString)
          }
          jedis.close()
        }
      }
    })

    //order流 要查询redis的user_info
    val saleDetailWithUserDS: DStream[SaleDetail] = saleDetailDS.mapPartitions {
      saleDetailitr => {
        val jedis: Jedis = RedisUtil.getJedisClient
        val saleDetailList = new ListBuffer[SaleDetail]
        for (saleDetail <- saleDetailitr) {
          val userKey: String = "user_info:" + saleDetail.user_id
          val userJson: String = jedis.get(userKey)
          val userinfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
          saleDetail.mergeUserInfo(userinfo)
          saleDetailList += saleDetail
        }
        jedis.close()
        //因为返回值类型是Iterator,所以要转换下
        saleDetailList.toIterator
      }
    }
    saleDetailWithUserDS.foreachRDD{
      rdd=>{
        rdd.foreachPartition{
          saleDetailItr=>{
            val saleDetailList: List[(String, SaleDetail)] = saleDetailItr.toList.map(
              saleDetail => (saleDetail.order_detail_id, saleDetail)
            )
            MyEsUtil.insertBulk(saleDetailList,GmallConstant.ES_INDEX_SALE)
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
