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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.util.Comparator.comparingLong;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingLong;

public class WordCountLocal {

    public static final String INPUT_FILE = "books";

    public static void main(String[] args) throws IOException {
        System.out.println("Reading from input: " + INPUT_FILE);
        LongSummaryStatistics jdk = measureWithWarmup(WordCountLocal::measureJdk);
        LongSummaryStatistics jet = measureWithWarmup(WordCountLocal::measureJet);
        System.out.println("JDK: " + jdk);
        System.out.println("Jet: " + jet);
    }

    private static long measureJet() {
        Map<String, Long> counts = new HashMap<>();
        long books = measure(() -> {
            Pattern delimiter = Pattern.compile("\\W+");
            Pipeline p = Pipeline.create();
            p.drawFrom(Sources.<String>files(INPUT_FILE))
             .flatMap(e ->
                     traverseArray(delimiter.split(e.toLowerCase()))
                             .filter(word -> !word.isEmpty()))
             .groupBy(wholeItem(), counting())
             .drainTo(Sinks.map(counts));
            p.run();
        });
        printResults(counts);
        return books;
    }

    private static long measureJdk()  {
        final Pattern delimiter = Pattern.compile("\\W+");
        long start = System.nanoTime();
        Stream<Path> knownSize = Stream.of(new File(INPUT_FILE).listFiles()).map(f -> f.toPath());
//        Stream<Path> unknownSize = Files.walk(Paths.get(INPUT_FILE))
//                                        .filter(p -> !p.toFile().isDirectory());
        Map<String, Long> counts =
                knownSize
                        .parallel()
                        .flatMap(book -> {
                            try {
                                return Files.lines(book);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .flatMap(line -> Arrays.stream(delimiter.split(line.toLowerCase())))
                        .filter(w -> !w.isEmpty())
                        .collect(groupingBy(identity(), Collectors.counting()));
        final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.print("done in " + took + " milliseconds.");
        printResults(counts);
        return took;
    }

    public static long measure(Runnable r) {
        long start = System.nanoTime();
        r.run();
        final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.println("done in " + took + " milliseconds.");
        return took;
    }

    public static LongSummaryStatistics measureWithWarmup(LongSupplier measure) {
        measure.getAsLong();
        measure.getAsLong();
        measure.getAsLong();

        List<Long> timings = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            timings.add(measure.getAsLong());
        }
        LongSummaryStatistics summary = timings.stream().collect(summarizingLong(x -> x));
        System.out.println(summary);
        return summary;
    }


    private static void printResults(Map<String, Long> counts) {
        final int limit = 10;
        System.out.format(" Top %d entries are:%n", limit);
        System.out.println("/-------+---------\\");
        System.out.println("| Count | Word    |");
        System.out.println("|-------+---------|");
        counts.entrySet().stream()
              .sorted(comparingLong(Entry<String, Long>::getValue).reversed())
              .limit(limit)
              .forEach(e -> System.out.format("|%6d | %-8s|%n", e.getValue(), e.getKey()));
        System.out.println("\\-------+---------/");
    }
}
