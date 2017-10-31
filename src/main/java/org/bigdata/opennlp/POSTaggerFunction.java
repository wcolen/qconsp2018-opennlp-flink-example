package org.bigdata.opennlp;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.postag.POSTaggerME;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class POSTaggerFunction<T> extends RichMapFunction<Annotation<T>,Annotation<T>> {

    private transient POSTagger posTagger;
    private final String model;

    public POSTaggerFunction(final String model) {
        this.model = model;
    }

    @Override
    public Annotation<T> map(Annotation<T> annotation) throws Exception {
        throw new RuntimeException("missing implementation");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        posTagger = new POSTaggerME(new POSModel(POSTaggerFunction.class.getResource(model)));
    }
}
