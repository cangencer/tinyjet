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

package com.hazelcast.jet.impl.processor;

import javax.annotation.Nonnull;

/**
 * Javadoc pending.
 */
public final class SnapshotKey {
    long timestamp;
    Object key;

    public SnapshotKey() {
    }

    SnapshotKey(long timestamp, @Nonnull Object key) {
        this.timestamp = timestamp;
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        SnapshotKey that;
        return this == o
                || o instanceof SnapshotKey
                && this.timestamp == (that = (SnapshotKey) o).timestamp
                && this.key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int hc = (int) (timestamp ^ (timestamp >>> 32));
        hc = 73 * hc + key.hashCode();
        return hc;
    }

    @Override
    public String toString() {
        return "SnapshotKey{timestamp=" + timestamp + ", key=" + key + '}';
    }
}
