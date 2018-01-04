package org.bigdata.opennlp;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;

public class ESSinkFunction implements ElasticsearchSinkFunction<Annotation> {

  private static Pattern ALPHANUMSPACE = Pattern.compile("[\\p{L}\\s]+");
  private static Set<String> BLACKLIST = Sets.newHashSet("gross margin");

  private Optional<String> entityKey(CharSequence mention) {

    String[] mentionTokens = SimpleTokenizer.INSTANCE.tokenize(mention.toString());

    StringBuilder sb = new StringBuilder();

    if (mentionTokens.length > 1) {

      // join and clean non alphanum
      int size = 0;
      for(String token : mentionTokens) {
        if (ALPHANUMSPACE.matcher(token).find()) {
          sb.append(token).append(" ");
          size++;
        }
      }
      if (size > 1) {
        String entity = sb.toString().toLowerCase().trim();
        if (!BLACKLIST.contains(entity))
          return Optional.of(entity);
      }
    }
    return Optional.empty();
  }

  private Collection<String> extractMentionKeys(Span[] sentences, String[][] mentions) {

    Collection<String> keys = new ArrayList<>();
    for (int i = 0; i < sentences.length; i++) {
      for (int j = 0; j < mentions[i].length; j++) {
        Optional<String> name = entityKey(mentions[i][j]);
        name.ifPresent(keys::add);
      }
    }

    return keys;
  }

  @Override
  public void process(Annotation element, RuntimeContext ctx, RequestIndexer indexer) {

    Map<String, Object> json = new HashMap<>();
    json.put("id", element.getId());
    json.put("text", element.getSofa());
    json.put("lang", element.getLanguage());
    json.put("date", element.getProperty("PUBLICATION_DATE"));
    json.put("source", element.getProperty("SOURCE"));
    json.put("headline", element.getSofa().substring(
      element.getHeadline().getStart(), element.getHeadline().getEnd()));

    json.put("person", extractMentionKeys(element.getSentences(), element.getPersonMention()));
    json.put("org", extractMentionKeys(element.getSentences(), element.getOrganizationMention()));
    json.put("location", extractMentionKeys(element.getSentences(), element.getLocationMention()));

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

    List<String> nounPhrases = new ArrayList<>();

    for (int i = 0; i < element.getSentences().length; i++) {
      String[] tokens = Span.spansToStrings(element.getTokens()[i], element.getSofa());
      for (int j = 0; j < element.getChunks()[i].length; j++) {
        Span chunkSpan = element.getChunks()[i][j];
        String[] chunkTokens = Span.spansToStrings(new Span[] {chunkSpan}, tokens);

        if (chunkSpan.getType().equalsIgnoreCase("NP")) {
          nounPhrases.add(chunkTokens[0]);
        }
      }
    }

    json.put("np", nounPhrases);

    IndexRequest request = Requests.indexRequest()
      .index("my-index")
      .type("news-article")
      .source(json);

    indexer.add(request);
  }
}
