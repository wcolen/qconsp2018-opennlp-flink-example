package org.bigdata.opennlp;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

public class TokenizerFunction<T> extends RichMapFunction<Annotation<T>,Annotation<T>> {

    private transient Tokenizer tokenizer;
    private final TokenizerModel model;

    public TokenizerFunction(final TokenizerModel model) {
        this.model = model;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tokenizer = new TokenizerME(model);
    }

    @Override
    public Annotation<T> map(Annotation<T> annotation) throws Exception {

        for (int i = 0; i < annotation.getSentences().length; i++) {
            Span sentence = annotation.getSentences()[i];
            CharSequence sentenceText = sentence.getCoveredText(annotation.getSofa());
            Span[] tokens = tokenizer.tokenizePos(sentenceText.toString());

            for (int j = 0; j < tokens.length; j++) {
                tokens[j] = new Span(tokens[j], sentence.getStart());
            }

            annotation.getTokens()[i] = tokens;
        }

        return annotation;
    }
}
