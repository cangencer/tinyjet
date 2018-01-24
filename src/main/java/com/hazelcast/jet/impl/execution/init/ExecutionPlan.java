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

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Edge.RoutingPolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.ConcurrentInboundEdgeStream;
import com.hazelcast.jet.impl.execution.ConveyorCollector;
import com.hazelcast.jet.impl.execution.InboundEdgeStream;
import com.hazelcast.jet.impl.execution.OutboundCollector;
import com.hazelcast.jet.impl.execution.OutboundEdgeStream;
import com.hazelcast.jet.impl.execution.ProcessorTasklet;
import com.hazelcast.jet.impl.execution.Tasklet;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.logging.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet.impl.execution.OutboundCollector.compositeCollector;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ExecutionPlan {

    private static final int PARTITION_COUNT = 271;

    private final List<Tasklet> tasklets = new ArrayList<>();

    private List<VertexDef> vertices = new ArrayList<>();

    private final Map<String, ConcurrentConveyor<Object>[]> localConveyorMap = new HashMap<>();
    private final List<Processor> processors = new ArrayList<>();


    ExecutionPlan() {
    }

    public void initialize() {
        initProcSuppliers();
        initDag();

        for (VertexDef srcVertex : vertices) {
            Collection<? extends Processor> processors = createProcessors(srcVertex, srcVertex.parallelism());

            int localProcessorIdx = 0;
            for (Processor p : processors) {
                int globalProcessorIndex = srcVertex.getProcIdxOffset() + localProcessorIdx;
                ProcCtx context = new ProcCtx(
                        JetInstance.EMPTY,
                        Logger.DEFAULT,
                        srcVertex.name(),
                        globalProcessorIndex,
                        ProcessingGuarantee.NONE);

                // createOutboundEdgeStreams() populates localConveyorMap and edgeSenderConveyorMap.
                // Also populates instance fields: senderMap, receiverMap, tasklets.
                List<OutboundEdgeStream> outboundStreams = createOutboundEdgeStreams(srcVertex, localProcessorIdx);
                List<InboundEdgeStream> inboundStreams = createInboundEdgeStreams(srcVertex, localProcessorIdx);
                ProcessorTasklet processorTasklet =
                        new ProcessorTasklet(context, p, inboundStreams, outboundStreams, 60_000);
                tasklets.add(processorTasklet);
                this.processors.add(p);
                localProcessorIdx++;
            }
        }
    }

    public List<ProcessorSupplier> getProcessorSuppliers() {
        return vertices.stream().map(VertexDef::processorSupplier).collect(toList());
    }

    public List<Tasklet> getTasklets() {
        return tasklets;
    }

    void addVertex(VertexDef vertex) {
        vertices.add(vertex);
    }

    private void initProcSuppliers() {
        for (VertexDef vertex : vertices) {
            ProcessorSupplier supplier = vertex.processorSupplier();
            supplier.init(new ProcSupplierCtx(JetInstance.EMPTY, Logger.DEFAULT, vertex.parallelism()));
        }
    }

    private void initDag() {
        final Map<Integer, VertexDef> vMap = vertices.stream().collect(toMap(VertexDef::vertexId, v -> v));
        vertices.forEach(v -> {
            v.inboundEdges().forEach(e -> e.initTransientFields(vMap, v, false));
            v.outboundEdges().forEach(e -> e.initTransientFields(vMap, v, true));
        });
        vertices.stream()
                .map(VertexDef::outboundEdges)
                .flatMap(List::stream)
                .map(EdgeDef::partitioner)
                .filter(Objects::nonNull)
                .forEach(p -> p.init(object -> {
                    return Math.floorMod(object.hashCode(), PARTITION_COUNT);
                }));
    }

    private static Collection<? extends Processor> createProcessors(VertexDef vertexDef, int parallelism) {
        final Collection<? extends Processor> processors = vertexDef.processorSupplier().get(parallelism);
        if (processors.size() != parallelism) {
            throw new JetException("ProcessorSupplier failed to return the requested number of processors." +
                    " Requested: " + parallelism + ", returned: " + processors.size());
        }
        return processors;
    }

    private List<OutboundEdgeStream> createOutboundEdgeStreams(VertexDef srcVertex, int processorIdx) {
        final List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
        for (EdgeDef edge : srcVertex.outboundEdges()) {
            outboundStreams.add(createOutboundEdgeStream(edge, processorIdx));
        }
        return outboundStreams;
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentConveyor<Object>[] createConveyorArray(int count, int queueCount, int queueSize) {
        ConcurrentConveyor<Object>[] concurrentConveyors = new ConcurrentConveyor[count];
        Arrays.setAll(concurrentConveyors, i -> {
            QueuedPipe<Object>[] queues = new QueuedPipe[queueCount];
            Arrays.setAll(queues, j -> new OneToOneConcurrentArrayQueue<>(queueSize));
            return concurrentConveyor(null, queues);
        });
        return concurrentConveyors;
    }

    private OutboundEdgeStream createOutboundEdgeStream(
            EdgeDef edge, int processorIndex
    ) {
        OutboundCollector[] outboundCollectors = createOutboundCollectors(edge, processorIndex);
        OutboundCollector compositeCollector = compositeCollector(outboundCollectors, edge, PARTITION_COUNT);
        return new OutboundEdgeStream(edge.sourceOrdinal(), compositeCollector);
    }

    private OutboundCollector[] createOutboundCollectors(
            EdgeDef edge, int processorIndex
    ) {
        final int upstreamParallelism = edge.sourceVertex().parallelism();
        final int downstreamParallelism = edge.destVertex().parallelism();
        final int queueSize = edge.getConfig().getQueueSize();

        final int[][] ptionsPerProcessor =
                PartitionArrangement.assignPartitionsToProcessors(downstreamParallelism, PARTITION_COUNT);

        if (edge.routingPolicy() == RoutingPolicy.ISOLATED) {
            if (downstreamParallelism < upstreamParallelism) {
                throw new IllegalArgumentException(String.format(
                        "The edge %s specifies the %s routing policy, but the downstream vertex" +
                                " parallelism (%d) is less than the upstream vertex parallelism (%d)",
                        edge, RoutingPolicy.ISOLATED.name(), downstreamParallelism, upstreamParallelism));
            }
            // there is only one producer per consumer for a one to many edge, so queueCount is always 1
            ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.computeIfAbsent(edge.edgeId(),
                    e -> createConveyorArray(downstreamParallelism, 1, queueSize));
            return IntStream.range(0, downstreamParallelism)
                            .filter(i -> i % upstreamParallelism == processorIndex)
                            .mapToObj(i -> new ConveyorCollector(localConveyors[i], 0, ptionsPerProcessor[i]))
                            .toArray(OutboundCollector[]::new);
        }

        /*
         * Each edge is represented by an array of conveyors between the producers and consumers
         * There are as many conveyors as there are consumers.
         * Each conveyor has one queue per producer.
         *
         * For a distributed edge, there is one additional producer per member represented
         * by the ReceiverTasklet.
         */
        final ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.computeIfAbsent(edge.edgeId(),
                e -> {
                    return createConveyorArray(downstreamParallelism, upstreamParallelism, queueSize);
                });
        final OutboundCollector[] localCollectors = new OutboundCollector[downstreamParallelism];
        Arrays.setAll(localCollectors, n ->
                new ConveyorCollector(localConveyors[n], processorIndex, ptionsPerProcessor[n]));

        // in a local edge, we only have the local collectors.
        return localCollectors;

    }

    private List<InboundEdgeStream> createInboundEdgeStreams(VertexDef srcVertex, int processorIdx) {
        final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
        for (EdgeDef inEdge : srcVertex.inboundEdges()) {
            // each tasklet has one input conveyor per edge
            final ConcurrentConveyor<Object> conveyor = localConveyorMap.get(inEdge.edgeId())[processorIdx];
            inboundStreams.add(newEdgeStream(inEdge, conveyor));
        }
        return inboundStreams;
    }

    private ConcurrentInboundEdgeStream newEdgeStream(EdgeDef inEdge, ConcurrentConveyor<Object> conveyor) {
        return new ConcurrentInboundEdgeStream(conveyor, inEdge.destOrdinal(), inEdge.priority(), 60_000);
    }

    public List<Processor> getProcessors() {
        return processors;
    }

}

