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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

/**
 * Factory of {@link ProcessorSupplier} instances. The starting point of
 * the chain leading to the eventual creation of {@code Processor} instances
 * on each cluster member:
 * <ol><li>
 * client creates {@code ProcessorMetaSupplier} as a part of the DAG;
 * </li><li>
 * serializes it and sends to a cluster member;
 * </li<li>
 * the member deserializes and uses it to create one {@code ProcessorSupplier}
 * for each cluster member;
 * </li><li>
 * serializes each {@code ProcessorSupplier} and sends it to its target member;
 * </li><li>
 * the target member deserializes and uses it to instantiate as many instances
 * of {@code Processor} as requested by the <em>parallelism</em> property on
 * the corresponding {@code Vertex}.
 * </li></ol>
 * Before being asked to create {@code ProcessorSupplier}s this meta-supplier will
 * be given access to the Hazelcast instance and, in particular, its cluster topology
 * and partitioning services. It can use the information from these services to
 * precisely parameterize each {@code Processor} instance that will be created on
 * each member.
 */
@FunctionalInterface
public interface ProcessorMetaSupplier extends Serializable {

    /**
     * Returns the local parallelism the vertex should be configured with.
     * The default implementation returns {@link
     * Vertex#LOCAL_PARALLELISM_USE_DEFAULT}.
     */
    default int preferredLocalParallelism() {
        return Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
    }

    /**
     * Called on the cluster member that receives the client request, after
     * deserializing the meta-supplier instance. Gives access to the Hazelcast
     * instance's services and provides the parallelism parameters determined
     * from the cluster size.
     */
    default void init(@Nonnull Context context) {
    }

    /**
     * Called to create a mapping from member {@link Address} to the
     * {@link ProcessorSupplier} that will be sent to that member. Jet calls
     * this method with a list of all cluster members' addresses and the
     * returned function must be a mapping that returns a non-null value for
     * each given address.
     * <p>
     * The method will be called once per job execution on the job's
     * <em>coordinator</em> member. {@code init()} will have already
     * been called.
     */
    @Nonnull
    Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses);

    /**
     * Called on coordinator member after execution has finished on all
     * members, successfully or not. This method will be called after {@link
     * ProcessorSupplier#complete(Throwable)} has been called on all
     * <em>available</em> members.
     * <p>
     * If there is an exception during the creation of the execution plan, this
     * method will be called regardless of whether the {@link #init(Context)
     * init()} or {@link #get(List) get()} method have been called or not.
     * <p>
     * If you rely on the fact that this method is run once per cluster, it can
     * happen that it is not called, if the coordinator member crashed.
     *
     * @param error the exception (if any) that caused the job to fail;
     *              {@code null} in the case of successful job completion
     */
    default void complete(Throwable error) {
    }

    /**
     * Factory method that wraps the given {@code ProcessorSupplier} and
     * returns the same instance for each given {@code Address}.
     *
     * @param procSupplier the processor supplier
     * @param preferredLocalParallelism the value to return from {@link #preferredLocalParallelism()}
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull ProcessorSupplier procSupplier, int preferredLocalParallelism) {
        return of((Address x) -> procSupplier, preferredLocalParallelism);
    }

    /**
     * Wraps the provided {@code ProcessorSupplier} into a meta-supplier that
     * will always return it. The {@link #preferredLocalParallelism()} of
     * the meta-supplier will be one, i.e., no local parallelization.
     */
    static ProcessorMetaSupplier dontParallelize(ProcessorSupplier supplier) {
        return of(supplier, 1);
    }

    /**
     * Wraps the provided {@code ProcessorSupplier} into a meta-supplier that
     * will always return it. The {@link #preferredLocalParallelism()} of
     * the meta-supplier will be {@link Vertex#LOCAL_PARALLELISM_USE_DEFAULT}.
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull ProcessorSupplier procSupplier) {
        return of(procSupplier, Vertex.LOCAL_PARALLELISM_USE_DEFAULT);
    }

    /**
     * Factory method that wraps the given {@code Supplier<Processor>}
     * and uses it as the supplier of all {@code Processor} instances.
     * Specifically, returns a meta-supplier that will always return the
     * result of calling {@link ProcessorSupplier#of(DistributedSupplier)}.
     */
    @Nonnull
    static ProcessorMetaSupplier dontParallelize(@Nonnull DistributedSupplier<? extends Processor> procSupplier) {
        return of(ProcessorSupplier.of(procSupplier), 1);
    }

    /**
     * Factory method that wraps the given {@code Supplier<Processor>}
     * and uses it as the supplier of all {@code Processor} instances.
     * Specifically, returns a meta-supplier that will always return the
     * result of calling {@link ProcessorSupplier#of(DistributedSupplier)}.
     *
     * @param procSupplier              the supplier of processors
     * @param preferredLocalParallelism the value to return from {@link #preferredLocalParallelism()}
     */
    @Nonnull
    static ProcessorMetaSupplier of(
            @Nonnull DistributedSupplier<? extends Processor> procSupplier, int preferredLocalParallelism
    ) {
        return of(ProcessorSupplier.of(procSupplier), preferredLocalParallelism);
    }

    /**
     * Factory method that wraps the given {@code Supplier<Processor>}
     * and uses it as the supplier of all {@code Processor} instances.
     * Specifically, returns a meta-supplier that will always return the
     * result of calling {@link ProcessorSupplier#of(DistributedSupplier)}.
     * The {@link #preferredLocalParallelism()} of the meta-supplier will be
     * {@link Vertex#LOCAL_PARALLELISM_USE_DEFAULT}.
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull DistributedSupplier<? extends Processor> procSupplier) {
        return of(procSupplier, Vertex.LOCAL_PARALLELISM_USE_DEFAULT);
    }

    /**
     * Factory method that creates a {@link ProcessorMetaSupplier} from the
     * supplied function that maps a cluster member address to a {@link
     * ProcessorSupplier}.
     *
     * @param addressToSupplier the mapping from address to ProcessorSupplier
     * @param preferredLocalParallelism the value to return from {@link #preferredLocalParallelism()}
     */
    static ProcessorMetaSupplier of(
            DistributedFunction<Address, ProcessorSupplier> addressToSupplier,
            int preferredLocalParallelism
    ) {
        return new ProcessorMetaSupplier() {
            @Override
            public int preferredLocalParallelism() {
                return preferredLocalParallelism;
            }

            @Nonnull @Override
            public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
                return addressToSupplier;
            }
        };
    }

    /**
     * Factory method that creates a {@link ProcessorMetaSupplier} from the
     * supplied function that maps a cluster member address to a {@link
     * ProcessorSupplier}. The {@link #preferredLocalParallelism()} of
     * the meta-supplier will be {@link Vertex#LOCAL_PARALLELISM_USE_DEFAULT}.
     */
    static ProcessorMetaSupplier of(DistributedFunction<Address, ProcessorSupplier> addressToSupplier) {
        return of(addressToSupplier, Vertex.LOCAL_PARALLELISM_USE_DEFAULT);
    }

    /**
     * Context passed to the meta-supplier at init time on the member that
     * received a job request from the client.
     */
    interface Context {

        /**
         * Returns the current Jet instance.
         */
        @Nonnull
        JetInstance jetInstance();

        /**
         * Returns the total number of {@code Processor}s that will be created
         * across the cluster. This number remains stable for entire job
         * execution.
         */
        int totalParallelism();

        /**
         * Returns the number of processors that each {@code ProcessorSupplier}
         * will be asked to create once deserialized on each member. All
         * members have equal local parallelism; dividing {@link
         * #totalParallelism} by local parallelism gives you the participating
         * member count. The count doesn't change unless the job restarts.
         */
        int localParallelism();

        /**
         * Returns a logger for the associated {@code ProcessorSupplier}.
         */
        @Nonnull
        ILogger logger();
    }

}