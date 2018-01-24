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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.util.QuickMath;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;

public class ReadSpliteratorP<T>  extends AbstractProcessor {

    private final Traverser<T> traverser;

    public ReadSpliteratorP(Spliterator<T> spliterator) {
        this.traverser = Traversers.traverseSpliterator(spliterator);
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    private static class ReadSpliteratorSupplier<T> implements ProcessorSupplier {

        private final Spliterator<T> spliterator;

        public ReadSpliteratorSupplier(Spliterator<T> spliterator) {
            this.spliterator = spliterator;
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            List<Spliterator<T>> splits = split(spliterator, QuickMath.log2(count));
            //TODO: when there are less splits than processors
            return splits.stream().map(ReadSpliteratorP::new).collect(Collectors.toList());
        }
    }

    public static <T> ProcessorSupplier readSpliterator(Spliterator<T> spliterator) {
        return new ReadSpliteratorSupplier<T>(spliterator);
    }

    public static <T> List<Spliterator<T>> split(Spliterator<T> iterator, int times) {
        return split(Collections.singletonList(iterator), times);
    }

    public static <T> List<Spliterator<T>> split(List<Spliterator<T>> input, int remaining) {
        if (remaining == 0) {
            return input;
        }
        List<Spliterator<T>> iterators = new ArrayList<>();
        for (Spliterator<T> original : input) {
            iterators.add(original);
            Spliterator<T> split = original.trySplit();
            if (split != null) {
                iterators.add(split);
            }
        }
        if (input.size() == iterators.size()) {
            return iterators;
        }
        return split(iterators, remaining - 1);
    }
}
