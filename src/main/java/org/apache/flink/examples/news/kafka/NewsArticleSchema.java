package org.apache.flink.examples.news.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.DeserializationException;

import java.io.IOException;

public class NewsArticleSchema implements DeserializationSchema<NewsArticle>, SerializationSchema<NewsArticle> {

  final ObjectMapper mapper = new ObjectMapper();

  @Override
  public NewsArticle deserialize(byte[] bytes) throws IOException {
    return mapper.readerFor(NewsArticle.class).readValue(bytes);
  }

  @Override
  public boolean isEndOfStream(NewsArticle newsArticle) {
    return false;
  }

  @Override
  public TypeInformation<NewsArticle> getProducedType() {
    return TypeInformation.of(NewsArticle.class);
  }

  @Override
  public byte[] serialize(NewsArticle newsArticle) {

    try {
      return mapper.writer().writeValueAsBytes(newsArticle);
    } catch (Exception ex) {
      throw new DeserializationException(ex);
    }

  }
}
