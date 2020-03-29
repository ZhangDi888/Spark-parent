package com.zhang.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.zhang.gmall0826.common.constat.GmallConstant
import com.zhang.gmall0826.realtime.been.StartupLog
import com.zhang.gmall0826.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._



object DauApp {

  def main(args: Array[String]): Unit = {
    /*
    * 求日活
    * 具体思路：1.创建上下文环境，从kafka消费中读取数据
    *
    * 2.对读取的数据进行结构转换
    * a.首先将json字符串转换成样例类
    * b.因为样例类中添加了日期和小时的字段，要补全这两个字段：先自定义转换的时间格式，然后把字符串的ts转换成日期格式放到自定义格式中
    * c.对新的日期做处理，同时补全字段
    * d.返回一个新的json字符串
    *
    * 3.
    * */
    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName("realtime").setMaster("local[*]")
    //Streaming5秒处理一次文件
    val ssc = new StreamingContext(conf,Seconds(5))
    //kafka从KAFKA_TOPIC_STARTUP，5秒获取一次数据
    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

    //数据流转换结构 变成case class 增加两个字段
    val recordMap: DStream[StartupLog] = recordDstream.map {
      record => {
        //取出读取kafka消费的数据，是一个json字符串
        val jsonString: String = record.value()
        //将字符串转换成calss class
        val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])
        //增加两个字段
        //首先new一个转化时间格式的对象，自定义事件格式
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
          //把日志的ts转换成日期格式
        val date = new Date(startupLog.ts)
        //把日期格式的ts转换成自定义的事件格式
        val str: String = simpleDateFormat.format(date)
        //按照空格切割，补全case class的两个时间字段
        val strings: Array[String] = str.split(" ")
        startupLog.logDate = strings(0)
        startupLog.logHour = strings(1)
        //返回日志
        startupLog
      }
    }

    //利用清单去重，如果清单里面已经有的就过滤掉
    //把今天的清单的数据拿到driver中，然后利用广播变量发到executor中，让每个executor自己核对，如果已经在清单中的就过滤掉
    val filteredDStream: DStream[StartupLog] = recordMap.transform {

      rdd =>
        println("过滤前：" + rdd.count())
        val jedis: Jedis = RedisUtil.getJedisClient
        //制作今天的日期
        val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        //制作今天的key
        val dauKey = "dau: " + today
        //从redis按照今天的key取值
        val setMid: util.Set[String] = jedis.smembers(dauKey)
        //关闭
        jedis.close()
        //通过广播变量发送到每个exeutor中
        val BC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(setMid)
        //过滤掉已经存在清单中的数据
        val filterRDD: RDD[StartupLog] = rdd.filter {
          StartupLog => {
            val setmid: util.Set[String] = BC.value
            !setmid.contains(StartupLog.mid)
          }
        }
        println("过滤后：" + rdd.count())
        filterRDD
    }
    //组内去重，取第一个
    val mapRDD: DStream[(String, StartupLog)] = filteredDStream.map(StartupLog => (StartupLog.mid,StartupLog))
    val realFilteredDS: DStream[StartupLog] = mapRDD.groupByKey().map {
      case (mid, s) =>
        val startupLogs: List[StartupLog] = s.toList.sortWith {
          (left, right) =>
            left.ts < right.ts
        }
        val startupLogList: List[StartupLog] = startupLogs.take(1)
        startupLogList(0)
    }

    //把当天的今日访问过的用户（mid）清单
    realFilteredDS.foreachRDD{
          //遍历RDD
      rdd =>
        //因为要连接redis，按区遍历RDD，创建连接效率高
        rdd.foreachPartition{startupLogItor=>
          //创建redis客户端
          val jedis: Jedis = RedisUtil.getJedisClient
          //遍历log，以dau+时间作为key，mid作为value，使用set类型（去重），添加进去
          for (startupLog <- startupLogItor) {
            var dauKey = "dau: " + startupLog.logDate
            //添加进redis
            jedis.sadd(dauKey,startupLog.mid)
            //设置过期时间
            jedis.expire(dauKey,60*60*24)
          }
          jedis.close()
        }
    }

    //将数据导入phoenix
    //需要在phoenix中创建字段一样的表
    realFilteredDS.foreachRDD{
      rdd => rdd.saveToPhoenix("GMALL0826_DAU",
        Seq("MID","UID","APPID","AREA","OS","CH","LOGTYPE","VS","LOGDATE","LOGHOUR","TS"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181")
      )
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
