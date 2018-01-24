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

import com.hazelcast.jet.accumulator.MutableReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.WordCountLocal.measure;
import static com.hazelcast.jet.WordCountLocal.measureWithWarmup;

public class FindPrimes {

    public static final int END_RANGE = 15_000_000;

    public static void main(String[] args) throws IOException {
        LongSummaryStatistics jdk = measureWithWarmup(FindPrimes::findPrimesJdk);
        LongSummaryStatistics jet = measureWithWarmup(FindPrimes::findPrimesJet);
        System.out.println("JDK: " + jdk);
        System.out.println("Jet: " + jet);
    }

    private static long findPrimesJet() {
        List<Integer> primes = new ArrayList<>();
        long val = measure(() -> {
            Stream<Integer> stream = IntStream.range(0, END_RANGE).boxed().parallel();
            Pipeline p = Pipeline.create();
            p.drawFrom(Sources.stream(stream))
             .filter(FindPrimes::isPrime)
             .drainTo(Sinks.list(primes));
            p.run();
        });
        System.out.println("Prime count: " + primes.size());
        System.out.println(primes.stream().limit(10).collect(Collectors.toList()));
        return val;
    }

    private static long findPrimesJdk() {
        final MutableReference<List<Integer>> primes = new MutableReference<>();
        long val = measure(() -> primes.set(IntStream
                .range(0, END_RANGE)
                .unordered()
                .parallel()
                .filter(FindPrimes::isPrime)
                .boxed()
                .collect(Collectors.toList())));
        System.out.println("Prime count: " + primes.get().size());
        System.out.println(primes.get().stream().limit(10).collect(Collectors.toList()));
        return val;
    }

    private static boolean isPrime(int n) {
        if (n <= 1) {
            return false;
        }

        int endValue = (int) Math.sqrt(n);
        for (int i = 2; i <= endValue; i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }
}
