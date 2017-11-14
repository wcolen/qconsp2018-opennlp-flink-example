package org.apache.flink.examples.news;

import com.fasterxml.jackson.databind.ObjectMapper;
import opennlp.tools.util.Span;
import org.bigdata.opennlp.Annotation;
import org.bigdata.opennlp.AnnotationFactory;

import java.io.IOException;
import java.io.Serializable;

public class NewsArticleAnnotationFactory extends AnnotationFactory implements Serializable {

  private static final String NEWLINE = "\n";
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Annotation createAnnotation(String jsonString) throws IOException{
    NewsArticle newsArticle = mapper.readValue(jsonString, NewsArticle.class);

    StringBuilder sb = new StringBuilder(newsArticle.getHeadline());
    newsArticle.getBody().forEach(paragraph -> sb.append(NEWLINE).append(paragraph));
    Annotation annotation = new Annotation();
    annotation.setId(newsArticle.getId());
    annotation.setSofa(sb.toString());
    annotation.setHeadline(new Span(0, newsArticle.getHeadline().length()));
    annotation.putProperty("SOURCE", newsArticle.getSourceName());
    annotation.putProperty("PUBLICATION_DATE", newsArticle.getPublicationDate());
    return annotation;
  }

  private NewsArticleAnnotationFactory() {}

  public static AnnotationFactory getFactory() {
    return new NewsArticleAnnotationFactory();
  }
}
