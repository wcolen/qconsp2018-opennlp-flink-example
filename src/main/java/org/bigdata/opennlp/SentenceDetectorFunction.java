package org.bigdata.opennlp;

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class SentenceDetectorFunction<T> extends RichMapFunction<Annotation<T>,Annotation<T>> {

    private transient SentenceDetector sentenceDetector;
    private final String model;

    public SentenceDetectorFunction(final String model) {
        this.model = model;
    }

    @Override
    public Annotation<T> map(Annotation<T> annotation) throws Exception {
        annotation.setSentences(sentenceDetector.sentPosDetect(annotation.getSofa()));
        // TODO: headline should be it own sentence. if first sentence is longer than headline then split first span into 2 spans
        return annotation;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sentenceDetector = new SentenceDetectorME(new SentenceModel(SentenceDetectorFunction.class.getResource(model)));
    }
}
