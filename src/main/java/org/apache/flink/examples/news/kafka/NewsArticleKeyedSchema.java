package org.apache.flink.examples.news.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

public class NewsArticleKeyedSchema implements KeyedDeserializationSchema<NewsArticle> {

  final ObjectMapper mapper = new ObjectMapper();

  @Override
  public NewsArticle deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
    return mapper.readerFor(NewsArticle.class).readValue(message);
  }

  @Override
  public boolean isEndOfStream(NewsArticle newsArticle) {
    return false;
  }

  @Override
  public TypeInformation<NewsArticle> getProducedType() {
    return TypeInformation.of(NewsArticle.class);
  }

}
