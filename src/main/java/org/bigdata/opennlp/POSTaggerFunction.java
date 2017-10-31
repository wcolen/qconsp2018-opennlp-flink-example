package org.bigdata.opennlp;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.postag.POSTaggerME;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class POSTaggerFunction<T> extends RichMapFunction<Annotation<T>,Annotation<T>> {

    private transient POSTagger posTagger;
    private final String modelPath;

    public POSTaggerFunction(final String modelPath) {
        this.modelPath = modelPath;
    }

    @Override
    public Annotation<T> map(Annotation<T> annotation) throws Exception {
        return annotation;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        posTagger = new POSTaggerME(new POSModel(POSTaggerFunction.class.getResource(modelPath)));
    }
}
