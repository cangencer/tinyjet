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

import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.Map;

public class WriteMapP<K, V> extends AbstractProcessor {
    private final Map<K, V> map;

    public WriteMapP(Map<K, V> map) {
        this.map = map;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry<K,V> entry = (Map.Entry<K,V>) item;
        map.put(entry.getKey(), entry.getValue());
        return true;
    }
}
