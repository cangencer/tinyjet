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

package com.hazelcast.jet.impl.execution.init;

import java.util.Arrays;

/**
 * Collaborator of {@link ExecutionPlan} that takes care of assigning
 * partition IDs to processors.
 */
final class PartitionArrangement {

    private PartitionArrangement() {

    }
    /**
     * Determines for each processor instance the partition IDs it will be in charge of
     * (processors are identified by their index). The method is called separately for
     * each edge. For a distributed edge, only partitions owned by the local member need
     * to be assigned; for a non-distributed edge, every partition ID must be assigned.
     * Local partitions will get the same assignments in both cases, and repeating the
     * invocation with the same arguments will always yield the same result.
     *
     * @param processorCount    number of processor instances
     * @param isEdgeDistributed whether the edge is distributed
     * @return a 2D-array where the major index is the index of a processor and
     * the {@code int[]} at that index is the array of partition IDs assigned to
     * the processor
     */
    static int[][] assignPartitionsToProcessors(int processorCount, int partitionCount) {
        int majorIndex = 0;
        int minorIndex = 0;
        final int[][] ptionsPerProcessor = createPtionArrays(partitionCount, processorCount);
        for (int i = 0; i < partitionCount; i++) {
            ptionsPerProcessor[majorIndex][minorIndex] = i;
            if (++majorIndex == processorCount) {
                majorIndex = 0;
                minorIndex++;
            }
        }
        return ptionsPerProcessor;
    }

    private static int[][] createPtionArrays(int ptionCount, int processorCount) {
        final int[][] ptionsPerProcessor = new int[processorCount][];
        final int quot = ptionCount / processorCount;
        final int rem = ptionCount % processorCount;
        Arrays.setAll(ptionsPerProcessor, i -> new int[quot + (i < rem ? 1 : 0)]);
        return ptionsPerProcessor;
    }
}
