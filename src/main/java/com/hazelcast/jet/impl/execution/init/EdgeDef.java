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

import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Edge.RoutingPolicy;
import com.hazelcast.jet.core.Partitioner;

import java.util.Map;

public class EdgeDef {

    private int oppositeVertexId;
    private int sourceOrdinal;
    private int destOrdinal;
    private int priority;
    private RoutingPolicy routingPolicy;
    private Partitioner partitioner;
    private EdgeConfig config;

    // transient fields populated and used after deserialization
    private transient String id;
    private transient VertexDef sourceVertex;
    private transient VertexDef destVertex;


    EdgeDef() {
    }

    EdgeDef(Edge edge, EdgeConfig config, int oppositeVertexId) {
        this.oppositeVertexId = oppositeVertexId;
        this.sourceOrdinal = edge.getSourceOrdinal();
        this.destOrdinal = edge.getDestOrdinal();
        this.priority = edge.getPriority();
        this.routingPolicy = edge.getRoutingPolicy();
        this.partitioner = edge.getPartitioner();
        this.config = config;
    }

    void initTransientFields(Map<Integer, VertexDef> vMap, VertexDef nearVertex, boolean isOutbound) {
        final VertexDef farVertex = vMap.get(oppositeVertexId);
        this.sourceVertex = isOutbound ? nearVertex : farVertex;
        this.destVertex = isOutbound ? farVertex : nearVertex;
        this.id = sourceVertex.vertexId() + ":" + destVertex.vertexId();
    }

    public RoutingPolicy routingPolicy() {
        return routingPolicy;
    }

    public Partitioner partitioner() {
        return partitioner;
    }

    String edgeId() {
        return id;
    }

    VertexDef sourceVertex() {
        return sourceVertex;
    }

    int sourceOrdinal() {
        return sourceOrdinal;
    }

    VertexDef destVertex() {
        return destVertex;
    }

    int destOrdinal() {
        return destOrdinal;
    }

    int priority() {
        return priority;
    }

    EdgeConfig getConfig() {
        return config;
    }

    @Override public String toString() {
        return String.format("%s(%d) -> %s(%d)", sourceVertex.name(), sourceOrdinal, destVertex.name(), destOrdinal);
    }
}
