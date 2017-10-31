package org.bigdata.opennlp;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class NameFinderFunction<T> extends RichMapFunction<Annotation<T>,Annotation<T>> {

    private final String model;
    private transient TokenNameFinder nameFinder;

    public NameFinderFunction(final String model) {
        this.model = model;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        nameFinder = new NameFinderME(new TokenNameFinderModel(NameFinderFunction.class.getResource(model)));
    }

    @Override
    public Annotation<T> map(Annotation<T> annotation) throws Exception {
        throw new RuntimeException("missing implementation");
    }
}
