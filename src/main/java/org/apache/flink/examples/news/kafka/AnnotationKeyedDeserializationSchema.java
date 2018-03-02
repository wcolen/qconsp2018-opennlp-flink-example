package org.apache.flink.examples.news.kafka;

import opennlp.tools.util.Span;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.bigdata.opennlp.Annotation;

import java.io.IOException;

public class AnnotationKeyedDeserializationSchema implements KeyedDeserializationSchema<Annotation> {

  private static final String NEWLINE = "\n";
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Annotation deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
    NewsArticle newsArticle = mapper.readerFor(NewsArticle.class).readValue(message);
    StringBuilder sb = new StringBuilder(newsArticle.getHeadline());
    newsArticle.getBody().forEach(paragraph -> sb.append(NEWLINE).append(paragraph));
    Annotation annotation = new Annotation();
    annotation.setId(newsArticle.getId());
    annotation.setSofa(sb.toString());
    annotation.setHeadline(new Span(0, newsArticle.getHeadline().length()));
    annotation.putProperty("SOURCE", newsArticle.getSourceName());
    annotation.putProperty("PUBLICATION_DATE", newsArticle.getPublicationDate());
    annotation.putProperty("URL", newsArticle.getUrl());
    return annotation;
  }

  @Override
  public boolean isEndOfStream(Annotation annotation) {
    return false;
  }

  @Override
  public TypeInformation<Annotation> getProducedType() {
    return TypeInformation.of(Annotation.class);
  }
}
