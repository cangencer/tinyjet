/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet;

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.groupingBy;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.util.Comparator.comparingDouble;

public class MarkovChainGenerator {

    public static final String INPUT_FILE = "books";
    public static final int CONSTANT_KEY = 1;

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        SortedMap<Double, String> initialStates = findInitialStates();
        Map<String, SortedMap<Double, String>> transitions = findStateTransitions();

        StringBuilder builder = new StringBuilder();
        String word = nextWord(initialStates);
        builder.append(capitalizeFirst(word));
        int numWordsInSentence = 0;
        for (int i = 0; i < 1000; i++) {
            SortedMap<Double, String> t = transitions.get(word);
            if (t == null || ++numWordsInSentence > 30) {
                word = nextWord(initialStates);
                builder.append(". ").append(capitalizeFirst(word));
                numWordsInSentence = 0;
            } else {
                word = nextWord(t);
                builder.append(" ").append(word);
            }
        }
        System.out.println(builder);
    }

    private static String nextWord(SortedMap<Double, String> transitions) {
        return transitions.tailMap(RANDOM.nextDouble()).values().iterator().next();
    }

    private static String capitalizeFirst(String word) {
        return word.substring(0, 1).toUpperCase() + word.substring(1);
    }

    private static SortedMap<Double, String> findInitialStates() {
        Map<String, Double> initialStateTransitions = new HashMap<>();
        Pattern firstWord = Pattern.compile("\\.\\s([a-z]+)");
        Pipeline p = Pipeline.create();
        ComputeStage<Entry<String, Long>> initialWords
                = p.drawFrom(Sources.<String>files(INPUT_FILE))
                   .flatMap(e -> traverseMatcher(firstWord.matcher(e.toLowerCase()), m -> m.group(1)))
                   .groupBy(wholeItem(), counting());

        ComputeStage<Long> totalWords = initialWords
                .groupBy(e -> "total", AggregateOperations.summingLong(Entry::getValue))
                .map(Entry::getValue);

        initialWords
                .hashJoin(totalWords, JoinClause.onKeys(e -> CONSTANT_KEY, e -> CONSTANT_KEY))
                .map(t -> entry(t.f0().getKey(), t.f0().getValue() / (double) t.f1()))
                .drainTo(Sinks.map(initialStateTransitions));
        p.run();
        printInitialStates(initialStateTransitions);

        // find cumulative probabilities
        double cumulative = 0;
        TreeMap<Double, String> probabilities = new TreeMap<>();
        for (Entry<String, Double> entry : initialStateTransitions.entrySet()) {
            cumulative += entry.getValue();
            probabilities.put(cumulative, entry.getKey());
        }
        return probabilities;
    }

    private static Map<String, SortedMap<Double, String>> findStateTransitions() {
        Map<String, SortedMap<Double, String>> stateTransitions = new HashMap<>();

        Pattern twoWords = Pattern.compile("(\\w+)\\s(\\w+)");
        Pipeline p = Pipeline.create();

        p.drawFrom(Sources.<String>files(INPUT_FILE, StandardCharsets.UTF_8, "edgar*"))
         .flatMap(e -> traverseMatcher(twoWords.matcher(e.toLowerCase()), m -> tuple2(m.group(1), m.group(2))))
         .groupBy(Tuple2::f0, buildAggregateOp())
         .drainTo(Sinks.map(stateTransitions));

        p.run();

        printStateTransitions(stateTransitions);
        return stateTransitions;
    }

    private static AggregateOperation1<Tuple2<String, String>, ?, SortedMap<Double, String>> buildAggregateOp() {
        AggregateOperation1<Tuple2<String, String>, List<Object>, List<Object>> aggrOp = allOf(
                counting(),
                groupingBy(Tuple2::f1, counting())
        );
        return aggrOp.withFinishFn(aggrOp.finishFn().andThen(l -> {
            long totals = (long) l.get(0);
            Map<String, Long> counts = (Map<String, Long>) l.get(1);
            SortedMap<Double, String> probabilities = new TreeMap<>();

            double cumulative = 0.0;
            for (Entry<String, Long> e : counts.entrySet()) {
                cumulative += e.getValue() / (double) totals;
                probabilities.put(cumulative, e.getKey());
            }
            return probabilities;
        }));
    }

    private static void printInitialStates(Map<String, Double> initialState) {
        final int limit = 10;
        System.out.format(" Top initial %d words are:%n", limit);
        System.out.println("/-------------+-------------\\");
        System.out.println("| Probability | Word        |");
        System.out.println("|-------------+-------------|");
        initialState.entrySet().stream()
                    .sorted(comparingDouble(Entry<String, Double>::getValue).reversed())
                    .limit(limit)
                    .forEach(e -> System.out.format("|  %.4f     | %-12s|%n", e.getValue(), e.getKey()));
        System.out.println("\\-------------+-------------/");
    }

    private static void printStateTransitions(Map<String, SortedMap<Double, String>> counts) {
        counts.entrySet().stream().limit(10).forEach(e -> {
            System.out.println("Transitions for: " + e.getKey());
            System.out.println("/-------------+-------------\\");
            System.out.println("| Probability | Word        |");
            System.out.println("|-------------+-------------|");
            e.getValue().entrySet().forEach(ee -> {
                System.out.format("|  %.4f     | %-12s|%n", ee.getKey(), ee.getValue());
            });
            System.out.println("\\-------------+-------------/");
        });
    }


    private static <R> Traverser<R> traverseMatcher(Matcher m, Function<Matcher, R> mapperFn) {
        AppendableTraverser<R> traverser = new AppendableTraverser<>(1);
        while (m.find()) {
            traverser.append(mapperFn.apply(m));
        }
        return traverser;
    }
}
