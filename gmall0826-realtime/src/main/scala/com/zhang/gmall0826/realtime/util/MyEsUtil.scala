package com.zhang.gmall0826.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    **/

  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    **/
  def close(client: JestClient): Unit = {
    if (client != null) {
      try {
        client.shutdownClient()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  /**
    * 建立连接
    **/
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
        .maxTotalConnection(20)//连接总数
        .connTimeout(10000).readTimeout(10000).build
    )
  }

  def insertBulk(souceList:List[(String,Any)],indexName:String){
    if(souceList != null && souceList.size > 0){
      //获取客户端
      val jest: JestClient = getClient
      //先将一批数据缓存在bulk中，提高效率，一批批处理
      val bulkBuilder = new Bulk.Builder
      for ((id,soucce) <- souceList) {
        //先单词插入index
        val index: Index = new Index.Builder(soucce).index(indexName).`type`("_doc").id(id).build()
        //将一条条数据插入容器中
        bulkBuilder.addAction(index)
      }
      //将这堆值build出来变成一个批次
      val bulk: Bulk = bulkBuilder.build()
      //获取执行的条数,execute表示执行的意思
      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
      println("保存了：" + items.size() + "条数据")
      jest.close()
    }

  }


def main(args: Array[String]): Unit = {
  val jest: JestClient = getClient
  val index: Index = new Index.Builder(Customer0826("zhang3",33)).index("customer_0826").`type`("ctype").id("1").build()
  jest.execute(index)
  jest.close()
}

  case class  Customer0826(name:String ,age:Int)
}
