package org.bigdata.opennlp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;

public class ESSinkFunction implements ElasticsearchSinkFunction<Annotation<NewsArticle>> {

  private static Predicate<String> ALPHANUMSPACE = Pattern.compile("[\\p{L}\\p{Nd}\\s]+").asPredicate();

  private String entityKey(CharSequence mention) {

    String[] mentionTokens = SimpleTokenizer.INSTANCE.tokenize(mention.toString());
    // join and clean non alphanum
    return Arrays.stream(mentionTokens)
            .filter(ALPHANUMSPACE)
            .map(t -> t.toLowerCase())
            //.sorted()
            .collect(Collectors.joining(" "));
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
			if (element.getEntityMention()[i].length > 1)
			  entityKeys.add(entityKey(name));
	  }
	}

	//json.put("entity-keys", entityKeys.toArray(new String[entityKeys.size()]));
	json.put("entity-keys", entityKeys);

	// create fields for words with certain pos tags ...
	// index nouns only

	List<String> nouns = new ArrayList<>();

    for (int i = 0; i < element.getSentences().length; i++) {
      String[] tokens = Span.spansToStrings(element.getTokens()[i], element.getSofa());
      String[] tags = element.getPos()[i];

      for (int j = 0; j < tokens.length; j++) {
        if (tags[j].startsWith("N")) {
          nouns.add(tokens[j]);
        }
      }
    }

	json.put("nouns", nouns);

	IndexRequest request = Requests.indexRequest()
		.index("my-index")
		.type("news-article")
		.source(json);

	indexer.add(request);
  }
}
