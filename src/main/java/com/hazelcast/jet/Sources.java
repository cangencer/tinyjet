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

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.SourceImpl;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.charset.Charset;
import java.util.Spliterator;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.processor.SourceProcessors.readFilesP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamFilesP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamSocketP;
import static com.hazelcast.jet.impl.connector.ReadSpliteratorP.readSpliterator;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains factory methods for various types of pipeline sources. To start
 * building a pipeline, pass a source to {@link Pipeline#drawFrom(Source)}
 * and you will obtain the initial {@link ComputeStage}. You can then
 * attach further stages to it.
 * <p>
 * The same pipeline may contain more than one source, each starting its
 * own branch. The branches may be merged with multiple-input transforms
 * such as co-group and hash-join.
 */
public final class Sources {

    private static final String GLOB_WILDCARD = "*";

    private Sources() {
    }

    /**
     * Returns a source constructed directly from the given Core API processor
     * meta-supplier.
     *
     * @param sourceName   user-friendly source name
     * @param metaSupplier the processor meta-supplier
     */
    public static <T> Source<T> fromProcessor(
            @Nonnull String sourceName,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        return new SourceImpl<>(sourceName, metaSupplier);
    }

    /**
     * Returns a source which connects to the specified socket and emits lines
     * of text received from it. It decodes the text using the supplied {@code
     * charset}.
     * <p>
     * Each underlying processor opens its own TCP connection, so there will be
     * {@code clusterSize * localParallelism} open connections to the server.
     * <p>
     * The source completes when the server closes the socket. It never attempts
     * to reconnect. Any {@code IOException} will cause the job to fail.
     * <p>
     * The source does not save any state to snapshot. On job restart, it will
     * emit whichever items the server sends. The implementation uses
     * non-blocking API, the processor is cooperative.
     */
    @Nonnull
    public static Source<String> socket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return fromProcessor("socketSourceSource(" + host + ':' + port + ')', streamSocketP(host, port, charset));
    }

    /**
     * A source that emits lines from files in a directory (but not its
     * subdirectories. The files must not change while being read; if they do,
     * the behavior is unspecified.
     * <p>
     * To be useful, the source should be configured to read data local to each
     * member. For example, if the pathname resolves to a shared network
     * filesystem visible by multiple members, they will emit duplicate data.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     *
     * @param directory parent directory of the files
     * @param charset   charset to use to decode the files
     * @param glob      the globbing mask, see {@link
     *                  java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *                  Use {@code "*"} for all files.
     */
    @Nonnull
    public static Source<String> files(
            @Nonnull String directory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return fromProcessor("filesSource(" + new File(directory, glob) + ')', readFilesP(directory, charset, glob));
    }

    /**
     * Convenience for {@link #files(String, Charset, String) readFiles(directory, UTF_8, "*")}.
     */
    public static Source<String> files(@Nonnull String directory) {
        return files(directory, UTF_8, GLOB_WILDCARD);
    }

    /**
     * A source that emits a stream of lines of text coming from files in
     * the watched directory (but not its subdirectories). It will emit only
     * new contents added after startup: both new files and new content
     * appended to existing ones.
     * <p>
     * To be useful, the source should be configured to read data local to each
     * member. For example, if the pathname resolves to a shared network
     * filesystem visible by multiple members, they will emit duplicate data.
     * <p>
     * If, during the scanning phase, the source observes a file that doesn't
     * end with a newline, it will assume that there is a line just being
     * written. This line won't appear in its output.
     * <p>
     * The source completes when the directory is deleted. However, in order
     * to delete the directory, all files in it must be deleted and if you
     * delete a file that is currently being read from, the job may encounter
     * an {@code IOException}. The directory must be deleted on all nodes.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * lines added after the restart will be emitted, which gives at-most-once
     * behavior.
     * <p>
     * <h3>Limitation on Windows</h3>
     * On Windows the {@code WatchService} is not notified of appended lines
     * until the file is closed. If the file-writing process keeps the file
     * open while appending, the processor may fail to observe the changes.
     * It will be notified if any process tries to open that file, such as
     * looking at the file in Explorer. This holds for Windows 10 with the NTFS
     * file system and might change in future. You are advised to do your own
     * testing on your target Windows platform.
     * <p>
     * <h3>Use the latest JRE</h3>
     * The underlying JDK API ({@link java.nio.file.WatchService}) has a
     * history of unreliability and this source may experience infinite
     * blocking, missed, or duplicate events as a result. Such problems may be
     * resolved by upgrading the JRE to the latest version.
     *
     * @param watchedDirectory pathname to the source directory
     * @param charset          charset to use to decode the files
     * @param glob             the globbing mask, see {@link
     *                         java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *                         Use {@code "*"} for all files.
     */
    public static Source<String> fileWatcher(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return fromProcessor("fileWatcherSource(" + watchedDirectory + '/' + glob + ')',
                streamFilesP(watchedDirectory, charset, glob)
        );
    }

    public static <T> Source<T> spliterator(@Nonnull Spliterator<T> spliterator) {
        return fromProcessor(
                System.identityHashCode(spliterator) + "",
                ProcessorMetaSupplier.of(readSpliterator(spliterator))
        );
    }

    public static <T> Source<T> stream(@Nonnull Stream<T> stream) {
        return spliterator(stream.spliterator());
    }

    public static <T> Source<T> iterable(@Nonnull Iterable<T> iterable) {
        return spliterator(iterable.spliterator());
    }

    /**
     * Convenience for {@link #fileWatcher(String, Charset, String)
     * streamFiles(watchedDirectory, UTF_8, "*")}.
     */
    public static Source<String> fileWatcher(@Nonnull String watchedDirectory) {
        return fileWatcher(watchedDirectory, UTF_8, GLOB_WILDCARD);
    }
}
