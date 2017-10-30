package org.apache.bigdata.opennlp

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.examples.news.NewsArticleInputFormat
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object NewsPipeline {

  /**
    *
    * @param args
    * --file <path to article.json.gz>
    * @throws Exception
    */
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment.setParallelism(1)

    val parameterTool = ParameterTool.fromArgs(args)

    val articleStream = env.readFile(new NewsArticleInputFormat, parameterTool.getRequired("file"))


    // TODO:



    env.execute
  }
}
