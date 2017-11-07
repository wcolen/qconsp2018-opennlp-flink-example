package org.bigdata.opennlp;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

public class NameFinderFunction<T> extends RichMapFunction<Annotation<T>,Annotation<T>> {

    private transient TokenNameFinder nameFinder;
    private final TokenNameFinderModel model;

    public NameFinderFunction(final TokenNameFinderModel model) {
        this.model = model;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        nameFinder = new NameFinderME(model);
    }

    @Override
    public Annotation<T> map(Annotation<T> annotation) throws Exception {

        for (int i = 0; i < annotation.getTokens().length; i++) {
            String[] tokens = Span.spansToStrings(annotation.getTokens()[i], annotation.getSofa());
            Span[] names = nameFinder.find(tokens);

            annotation.getEntityMention()[i] = Span.spansToStrings(names, tokens);
        }

        return annotation;
    }
}
