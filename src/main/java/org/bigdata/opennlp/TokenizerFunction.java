package org.bigdata.opennlp;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class TokenizerFunction<T> extends RichMapFunction<Annotation<T>,Annotation<T>> {

    private transient Tokenizer tokenizer;
    private final String model;

    public TokenizerFunction(final String model) {
        this.model = model;
    }

    @Override
    public Annotation<T> map(Annotation<T> annotation) throws Exception {
        annotation.setTokens(tokenizer.tokenizePos(annotation.getSofa()));
        return annotation;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tokenizer = new TokenizerME(new TokenizerModel(TokenizerFunction.class.getResource(model)));
    }
}
