package org.bigdata.opennlp;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

public class ESSinkFunction<T> implements ElasticsearchSinkFunction<Annotation<T>> {

  private final String index;
  private final String type;

  public ESSinkFunction(final String index, final String type) {
    this.index = index;
    this.type = type;
  }

  @Override
  public void process(Annotation<T> annotation, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
    requestIndexer.add(createIndexRequest(annotation));
  }

  public IndexRequest createIndexRequest(Annotation<T> annotation) {
    Map<String, String> mapping = new HashMap<>();
    mapping.put("id", annotation.getId());
    mapping.put("headline", annotation.getSofa().substring(annotation.getHeadline().getStart(), annotation.getHeadline().getEnd()));
    /*
    map more fields
     */

    return Requests.indexRequest()
            .index(index)
            .type(type)
            .source(mapping);
  }

}
