package org.bigdata.opennlp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;

public class ESSinkFunction implements ElasticsearchSinkFunction<Annotation<NewsArticle>> {

  private String entityKey(CharSequence mention) {

    String[] mentionTokens = SimpleTokenizer.INSTANCE.tokenize(mention.toString());
	Arrays.sort(mentionTokens);

	StringBuilder key = new StringBuilder();

	for (String token : mentionTokens) {
	  key.append(token.toLowerCase());
	}

	return key.toString();
  }

  @Override
  public void process(Annotation<NewsArticle> element, RuntimeContext ctx, RequestIndexer indexer) {

	Map<String, Object> json = new HashMap<>();
	json.put("id", element.getId());
	json.put("text", element.getSofa());
	json.put("headline", element.getSofa().substring(
		element.getHeadline().getStart(), element.getHeadline().getEnd()));

	List<String> entityKeys = new ArrayList<>();

	for (int i = 0; i < element.getSentences().length; i++) {

	  for (int j = 0; j < element.getEntityMention()[i].length; j++) {
		String name = element.getEntityMention()[i][j];
		entityKeys.add(entityKey(name));
	  }
	}

	json.put("entity-keys", entityKeys);

	IndexRequest request = Requests.indexRequest()
		.index("my-index")
		.type("news-article")
		.source(json);

	indexer.add(request);
  }
}
