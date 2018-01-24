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

import com.hazelcast.jet.core.ProcessorSupplier;

import java.util.ArrayList;
import java.util.List;

public class VertexDef {

    private int id;
    private List<EdgeDef> inboundEdges = new ArrayList<>();
    private List<EdgeDef> outboundEdges = new ArrayList<>();
    private String name;
    private ProcessorSupplier processorSupplier;
    private int procIdxOffset;
    private int parallelism;

    VertexDef() {
    }

    VertexDef(int id, String name, ProcessorSupplier processorSupplier,
              int procIdxOffset, int parallelism) {
        this.id = id;
        this.name = name;
        this.processorSupplier = processorSupplier;
        this.procIdxOffset = procIdxOffset;
        this.parallelism = parallelism;
    }

    String name() {
        return name;
    }

    int parallelism() {
        return parallelism;
    }

    int vertexId() {
        return id;
    }

    void addInboundEdges(List<EdgeDef> edges) {
        this.inboundEdges.addAll(edges);
    }

    void addOutboundEdges(List<EdgeDef> edges) {
        this.outboundEdges.addAll(edges);
    }

    List<EdgeDef> inboundEdges() {
        return inboundEdges;
    }

    List<EdgeDef> outboundEdges() {
        return outboundEdges;
    }

    ProcessorSupplier processorSupplier() {
        return processorSupplier;
    }

    int getProcIdxOffset() {
        return procIdxOffset;
    }

    @Override
    public String toString() {
        return "VertexDef{" +
                "name='" + name + '\'' +
                '}';
    }
}
