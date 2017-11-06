package org.bigdata.opennlp;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

public class ESSinkFunction implements ElasticsearchSinkFunction<Annotation<NewsArticle>> {

  @Override
  public void process(Annotation<NewsArticle> element, RuntimeContext ctx, RequestIndexer indexer) {

	Map<String, String> json = new HashMap<>();
	json.put("id", element.getId());
	json.put("text", element.getSofa());
	json.put("headline", element.getSofa().substring(
		element.getHeadline().getStart(), element.getHeadline().getEnd()));

	IndexRequest request = Requests.indexRequest()
		.index("my-index")
		.type("news-article")
		.source(json);

	indexer.add(request);
  }
}
