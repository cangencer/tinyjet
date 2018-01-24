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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.lang.Integer.min;
import static java.util.stream.Collectors.toList;

public final class ExecutionPlanBuilder {

    public static final Address LOCAL = new Address("", 0);

    private ExecutionPlanBuilder() {
    }

    public static ExecutionPlan createExecPlan(int parallelism, DAG dag) {
        final EdgeConfig defaultEdgeConfig = new EdgeConfig();
        final Map<String, Integer> vertexIdMap = assignVertexIds(dag);
        final ExecutionPlan plan = new ExecutionPlan();
        for (Entry<String, Integer> entry : vertexIdMap.entrySet()) {
            final Vertex vertex = dag.getVertex(entry.getKey());
            final ProcessorMetaSupplier metaSupplier = vertex.getMetaSupplier();
            final int vertexId = entry.getValue();
            final int localParallelism = determineParallelism(vertex,
                    metaSupplier.preferredLocalParallelism(), parallelism);
            final List<EdgeDef> inbound = toEdgeDefs(dag.getInboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getSourceName()));
            final List<EdgeDef> outbound = toEdgeDefs(dag.getOutboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getDestName()));
            final ILogger logger = Logger.DEFAULT;
            metaSupplier.init(new MetaSupplierCtx(JetInstance.EMPTY, logger, localParallelism, localParallelism));

            Function<Address, ProcessorSupplier> procSupplierFn = metaSupplier.get(
                    Collections.singletonList(LOCAL)
            );

            final ProcessorSupplier processorSupplier = procSupplierFn.apply(LOCAL);
            final VertexDef vertexDef = new VertexDef(
                    vertexId, vertex.getName(), processorSupplier, 0, localParallelism);
            vertexDef.addInboundEdges(inbound);
            vertexDef.addOutboundEdges(outbound);
            plan.addVertex(vertexDef);
        }
        return plan;
    }

    private static Map<String, Integer> assignVertexIds(DAG dag) {
        Map<String, Integer> vertexIdMap = new LinkedHashMap<>();
        final int[] vertexId = {0};
        dag.forEach(v -> vertexIdMap.put(v.getName(), vertexId[0]++));
        return vertexIdMap;
    }

    private static int determineParallelism(Vertex vertex, int preferredLocalParallelism, int defaultParallelism) {
        if (!Vertex.isValidLocalParallelism(preferredLocalParallelism)) {
            throw new JetException(String.format(
                    "ProcessorMetaSupplier in vertex %s specifies preferred local parallelism of %d",
                    vertex.getName(), preferredLocalParallelism));
        }
        int localParallelism = vertex.getLocalParallelism();
        if (!Vertex.isValidLocalParallelism(localParallelism)) {
            throw new JetException(String.format(
                    "Vertex %s specifies local parallelism of %d", vertex.getName(), localParallelism));
        }
        return localParallelism != LOCAL_PARALLELISM_USE_DEFAULT
                ? localParallelism
                : preferredLocalParallelism != LOCAL_PARALLELISM_USE_DEFAULT
                ? min(preferredLocalParallelism, defaultParallelism)
                : defaultParallelism;
    }

    private static List<EdgeDef> toEdgeDefs(
            List<Edge> edges, EdgeConfig defaultEdgeConfig,
            Function<Edge, Integer> oppositeVtxId) {
        return edges.stream()
                    .map(edge -> new EdgeDef(edge, edge.getConfig() == null ? defaultEdgeConfig : edge.getConfig(),
                            oppositeVtxId.apply(edge)))
                    .collect(toList());
    }
}
