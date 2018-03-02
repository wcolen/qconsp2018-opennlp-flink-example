package org.bigdata.opennlp.functions;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import opennlp.tools.util.StringUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.calcite.shaded.com.google.common.base.Joiner;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.bigdata.opennlp.Annotation;

public class NameFinderFunction extends RichMapFunction<Annotation,Annotation> {

    private static final Pattern tokenCountPattern = Pattern.compile("\\b\\S\\b\\s*");
    private static final Pattern genetiv = Pattern.compile("(’s|'s)");
    private static final Pattern tailingPeriod = Pattern.compile("\\b\\p{L}*\\W$");

    private transient TokenNameFinder nameFinder;
    private final TokenNameFinderModel model;

    // lists used for cleaning the entities... as the general NER model is a bit messy...
    private static Set<String> personBlacklist;
    private static Set<String> organizationBlacklist;
    private static Set<String> personStopwords;
    private static Set<String> organizationStopwords;

    public NameFinderFunction(final TokenNameFinderModel model) throws IOException {
        this.model = model;
        personBlacklist = Sets.newHashSet(IOUtils.readLines(
                NameFinderFunction.class.getResourceAsStream("/blacklists/persons")));
        personStopwords = Sets.newHashSet(IOUtils.readLines(
                NameFinderFunction.class.getResourceAsStream("/blacklists/person-stopwords")));
        organizationBlacklist = Sets.newHashSet(IOUtils.readLines(
                NameFinderFunction.class.getResourceAsStream("/blacklists/organizations")));
        organizationStopwords = Sets.newHashSet(IOUtils.readLines(
                NameFinderFunction.class.getResourceAsStream("/blacklists/org-stopwords")));
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

                    String person = getCleanPersonEntity(tokens, nameSpans[j]);

                    if (!person.isEmpty() && tokenCount(person) > 1) {
                        personMentions.add(person);
                    }

                }
                else if (nameSpans[j].getType().startsWith("org")) {
                    if (!organizationBlacklist.contains(names[j])) {
                        boolean found = false;
                        for (int x = nameSpans[j].getStart(); x < nameSpans[j].getEnd(); x++) {
                            if (organizationStopwords.contains(tokens[x])) {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                            organizationMentions.add(names[j]);
                    }
                }
                else if ("gpe".equals(nameSpans[j].getType()) || "loc".equals(nameSpans[j].getType())
                    || "place".equals(nameSpans[j].getType())) {
                    locationMentions.add(names[j]);
                }
            }

            annotation.getPersonMentions()[i] =
                personMentions.toArray(new String[personMentions.size()]);

            annotation.getOrganizationMentions()[i] =
                organizationMentions.toArray(new String[organizationMentions.size()]);

            annotation.getLocationMentions()[i] =
                locationMentions.toArray(new String[locationMentions.size()]);
        }

        return annotation;
    }

    private static int tokenCount(String text) {
        return tokenCountPattern
                .matcher(text.replaceAll("\\.$", ""))
                .replaceAll("")
                .split("\\s+")
                .length;
    }

    String getCleanPersonEntity(String[] tokens, Span span) {
        int spanLength = span.getEnd() - span.getStart();

        if (spanLength <= 1)
            return "";

        String[] personTokens = new String[spanLength];
        for (int i = 0; i < spanLength; i++) {
            String token = tokens[span.getStart() + i];
            if (!personStopwords.contains(token)) {
                token = genetiv.matcher(token).replaceAll("");
            } else {
                token = "";
            }
            personTokens[i] = token;
        }

        String person = Arrays.stream(personTokens)
                .filter(t -> !t.isEmpty())
                .collect(Collectors.joining(" "));

        int c = 0;
        while (person.endsWith(".") && c < 10) { // magic number to prevent infinite loop
            person = tailingPeriod.matcher(person).replaceAll("").trim();
            c++;
        }

        if (!personBlacklist.contains(StringUtil.toLowerCase(person)))
            return person;
        else
            return "";
    }
}
