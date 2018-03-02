package org.bigdata.opennlp;


import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;

public class ESSinkFunction implements ElasticsearchSinkFunction<Annotation> {

  private static Pattern ALPHANUMSPACE = Pattern.compile("[\\p{L}\\s]+");

  private static final Pattern filter = Pattern.compile("[\\p{L}\\s]+");

  private Optional<String> entityKey(CharSequence mention) {

    String[] mentionTokens = SimpleTokenizer.INSTANCE.tokenize(mention.toString());

    StringBuilder sb = new StringBuilder();

    if (mentionTokens.length > 1) {

      // get rid of name1 + sigle name2
      if (!(mentionTokens[1].length() == 1 && mentionTokens.length == 1)) {

        // join and clean non alphanum
        int size = 0;
        for (String token : mentionTokens) {
          if (ALPHANUMSPACE.matcher(token).find()) {
            sb.append(token).append(" ");
            size++;
          }
        }
        if (size > 1) {
          String entity = sb.toString().trim();
          return Optional.of(entity);
        }
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

  private Collection<Map<String,String>> extractPersonKeys(Span[] sentences, String[][] mentions, String[][] genders) {

    Collection<Map<String,String>> keys = new ArrayList<>();
    for (int i = 0; i < sentences.length; i++) {
      for (int j = 0; j < mentions[i].length; j++) {
        Optional<String> name = entityKey(mentions[i][j]);
        if (name.isPresent()) {
          Map<String,String> person = new HashMap<>();
          person.put("gender", genders[i][j]);
          person.put("name", name.get());
          keys.add(person);
        }
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
    json.put("topic", element.getTopic());
    json.put("sentiment", element.getSentiment());
    json.put("sentimentSum", element.getSentimentSum());
    json.put("date", element.getProperty("PUBLICATION_DATE"));
    json.put("source", element.getProperty("SOURCE"));
    json.put("headline", element.getSofa().substring(
      element.getHeadline().getStart(), element.getHeadline().getEnd()));

    json.put("person", extractPersonKeys(element.getSentences(), element.getPersonMentions(), element.getPersonGenders()));
    json.put("org", extractMentionKeys(element.getSentences(), element.getOrganizationMentions()));
    json.put("location", extractMentionKeys(element.getSentences(), element.getLocationMentions()));

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

    // sort nouns
    Map<String,Long> nounsCount = nouns.stream().collect(Collectors.groupingBy(Function.identity(), counting()));
    nouns = nounsCount.entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .map(Map.Entry::getKey)
            .filter(n -> filter.matcher(n).matches())
            .collect(Collectors.toList());
    json.put("nouns", nouns);

    List<String> nounPhrases = new ArrayList<>();
    for (int i = 0; i < element.getSentences().length; i++) {
      String[] tokens = Span.spansToStrings(element.getTokens()[i], element.getSofa());
      for (int j = 0; j < element.getChunks()[i].length; j++) {
        Span chunkSpan = element.getChunks()[i][j];
        String[] chunkTokens = Span.spansToStrings(new Span[] {chunkSpan}, tokens);

        if (chunkSpan.getType().equalsIgnoreCase("NP") && (chunkSpan.getEnd() - chunkSpan.getStart() >= 3)) {
          nounPhrases.add(chunkTokens[0]);
        }
      }
    }

    // sort noun phrases
    Map<String,Long> nounPhrasesCount = nounPhrases.stream().collect(Collectors.groupingBy(Function.identity(), counting()));
    nounPhrases = nounPhrasesCount.entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .map(Map.Entry::getKey)
            .filter(np -> filter.matcher(np).matches())
            .collect(Collectors.toList());
    json.put("np", nounPhrases);

    IndexRequest request = Requests.indexRequest()
      .index("my-index")
      .type("news-article")
      .source(json);

    indexer.add(request);
  }

}
