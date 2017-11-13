package org.bigdata.opennlp;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.chunker.Chunker;
import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.util.Span;

public class ChunkerFunction<T> extends RichMapFunction<Annotation<T>,Annotation<T>> {

  private ChunkerModel model;
  private Chunker chunker;

  ChunkerFunction(ChunkerModel model) {
	this.model = model;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
	  super.open(parameters);
	  chunker = new ChunkerME(model);
  }

  @Override
  public Annotation<T> map(Annotation<T> annotation) throws Exception {
	for (int i = 0; i < annotation.getTokens().length; i++) {
	  String[] tokens = Span.spansToStrings(annotation.getTokens()[i], annotation.getSofa());
	  String[] tags = annotation.getPos()[i];
	  String[] chunks = chunker.chunk(tokens, tags);

	  annotation.getChunks()[i] = chunks;
	}

	return annotation;
  }
}
