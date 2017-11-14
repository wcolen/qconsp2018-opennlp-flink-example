package org.bigdata.opennlp;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

public class NameFinderFunction extends RichMapFunction<Annotation,Annotation> {

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
    public Annotation map(Annotation annotation) throws Exception {

        nameFinder.clearAdaptiveData();
        for (int i = 0; i < annotation.getTokens().length; i++) {

            List<String> personMentions = new ArrayList<>();
            List<String> organizationMentions = new ArrayList<>();
            List<String> locationMentions = new ArrayList<>();

            String[] tokens = Span.spansToStrings(annotation.getTokens()[i], annotation.getSofa());
            Span[] nameSpans = nameFinder.find(tokens);
            String[] names = Span.spansToStrings(nameSpans, tokens);

            for (int j = 0; j < nameSpans.length; j++) {
                if (nameSpans[j].getType().equals("person")) {
                    personMentions.add(names[j]);
                }
                else if (nameSpans[j].getType().startsWith("org")) {
                    organizationMentions.add(names[j]);
                }
                else if ("gpe".equals(nameSpans[j].getType()) || "loc".equals(nameSpans[j].getType())
                    || "place".equals(nameSpans[j].getType())) {
                    locationMentions.add(names[j]);
                }
            }

            annotation.getPersonMention()[i] =
                personMentions.toArray(new String[personMentions.size()]);

            annotation.getOrganizationMention()[i] =
                organizationMentions.toArray(new String[organizationMentions.size()]);

            annotation.getLocationMention()[i] =
                locationMentions.toArray(new String[locationMentions.size()]);
        }

        return annotation;
    }
}
