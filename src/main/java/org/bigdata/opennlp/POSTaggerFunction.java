package org.bigdata.opennlp;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.util.Span;

public class POSTaggerFunction extends RichMapFunction<Annotation,Annotation> {

    private transient POSTagger posTagger;

    private final POSModel model;

    public POSTaggerFunction(final POSModel model) {
        this.model = model;
    }

    public void open(Configuration parameters) throws Exception {
        posTagger = new POSTaggerME(model);
    }

    @Override
    public Annotation map(Annotation annotation) throws Exception {

        for (int i = 0; i < annotation.getTokens().length; i++) {
            String[] tokens = Span.spansToStrings(annotation.getTokens()[i], annotation.getSofa());
            annotation.getPos()[i] = posTagger.tag(tokens);
        }

        return annotation;
    }
}
