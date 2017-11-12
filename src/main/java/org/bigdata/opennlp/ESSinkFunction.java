package org.bigdata.opennlp;

import opennlp.tools.tokenize.SimpleTokenizer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;
import java.util.stream.Collectors;

public class ESSinkFunction implements ElasticsearchSinkFunction<Annotation<NewsArticle>> {

  private String entityKey(CharSequence mention) {

    String[] mentionTokens = SimpleTokenizer.INSTANCE.tokenize(mention.toString());
    // join and clean non alphanum
    return Arrays.stream(mentionTokens)
            .map(t -> t.toLowerCase()).sorted()
            .collect(Collectors.joining(" "))
            .replaceAll("[^\\p{L}\\p{Nd}]+", "");
		/*
		Arrays.sort(mentionTokens);

		StringBuilder key = new StringBuilder();

		for (String token : mentionTokens) {
			key.append(token.toLowerCase());
		}

		return key.toString();
		*/
  }

  @Override
  public void process(Annotation<NewsArticle> element, RuntimeContext ctx, RequestIndexer indexer) {

	Map<String, Object> json = new HashMap<>();
	json.put("id", element.getId());
	json.put("text", element.getSofa());
	json.put("lang", element.getLanguage());
	json.put("date", element.getPiggyback().getPublicationDate());
	json.put("source", element.getPiggyback().getSourceName());
	json.put("headline", element.getSofa().substring(
		element.getHeadline().getStart(), element.getHeadline().getEnd()));

	List<String> entityKeys = new ArrayList<>();

	for (int i = 0; i < element.getSentences().length; i++) {

	  for (int j = 0; j < element.getEntityMention()[i].length; j++) {
		String name = element.getEntityMention()[i][j];
		entityKeys.add(entityKey(name));
	  }
	}

	json.put("entity-keys", entityKeys.toArray(new String[entityKeys.size()]));

	IndexRequest request = Requests.indexRequest()
		.index("my-index")
		.type("news-article")
		.source(json);

	indexer.add(request);
  }
}
