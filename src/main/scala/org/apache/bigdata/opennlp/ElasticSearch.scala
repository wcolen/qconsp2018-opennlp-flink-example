package org.apache.bigdata.opennlp

import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.Properties

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.hs.opennlp.NameSample

case class ESArticle(id: String, lang: String, nlpLang: String, cybozuLang: String, text: String, body: String, misclass: Boolean)

object ElasticSearch {

  //val config = new java.util.HashMap[String, String]
  //config.put("cluster.name", "docker-cluster")
  //config.put("bulk.flush.max.actions", "100")

  val config = {
    val map = new java.util.HashMap[String, String]
    val props: Properties = new Properties()
    props.load(getClass.getResourceAsStream("/elastic.properties"))
    val ite = props.stringPropertyNames().iterator()
    while(ite.hasNext) {
      val propname = ite.next()
      map.put(propname, props.getProperty(propname))
    }
    map
  }

  val transportAddresses = new java.util.ArrayList[InetSocketAddress]
  config.get("hosts").split(",").foreach(_.split(":") match {
    case Array(host, port) =>
      transportAddresses.add(new InetSocketAddress(InetAddress.getByName(host), port.toInt))
  })

  //transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))
  //transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9301))

  lazy val SINK =
    new ElasticsearchSink(config, transportAddresses,

      new ElasticsearchSinkFunction[NameSample] {

        def createIndexRequest(element: NameSample): IndexRequest = {
          val mapping = new util.HashMap[String, AnyRef]
          mapping.put("article_id", element.getId)
          mapping.put("sentence", element.getSentence)

          // problem: wrong order of fields, id seems to be wrong type in general, as well as retweetCount
          Requests.indexRequest.index(config.get("index")).`type`(config.get("type")).source(mapping)
        }

        override def process(element: NameSample, ctx: RuntimeContext, indexer: RequestIndexer)
        {
          try{
            indexer.add(createIndexRequest(element))
          } catch {
            case e:Exception => println{
              println("an exception occurred: " + ExceptionUtils.getStackTrace(e))
            }
            case _:Throwable => println("Got some other kind of exception")
          }
        }

      })
}
