/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.stimulussoft.filequeue.store;


import com.google.common.base.Preconditions;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FilePath;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Fast queue implementation on top of MVStore. This class is thread-safe.
 *
 * @author Valentin Popov
 * @author Jamie Band
 * Thanks for Martin Grotze for his original work on Persistent Queue
 */

public class MVStoreQueue implements Comparable<MVStoreQueue> {

    private final String queueName;
    private MVMap<Integer, byte[]> mvMap;
    private MVStore store;
    private final Path queueDir;
    private final AtomicInteger tailKey = new AtomicInteger(0);

    static {
        FilePath.register(new JimFSDecorator());
    }

    /**
     * This class is for internal use only. Please refer to FileQueue class.
     *
     * @param queueDir  filequeue database environment directory path
     * @param queueName descriptive filequeue name
     * @throws IOException thrown when the given queueEnvPath does not exist and cannot be created.
     */
    public MVStoreQueue(final Path queueDir,
                        final String queueName) throws IOException {
        Files.createDirectories(queueDir);
        this.queueDir = queueDir.toAbsolutePath();
        this.queueName = queueName;
        reopen();
    }

    private Path getDBName() {
        return queueDir.resolve(queueName);
    }

    public synchronized void reopen() throws IllegalStateException {
        try {
            if (store != null && !store.isClosed()) store.close();
        } catch (Exception ignored) {
        }
        store = getOpenStore();
        mvMap = store.openMap(queueName);
        if (!mvMap.isEmpty())
            tailKey.set(mvMap.lastKey());

    }

    private MVStore getOpenStore() {
        Path dbName = getDBName();
        String path = dbName.toUri().getScheme().equals("jimfs") ?
                dbName.toUri().toString() : dbName.toString();
        return new MVStore.Builder().fileName(path).cacheSize(1).autoCommitDisabled().open();
    }

    public Path getQueueDir() {
        return queueDir;
    }

    /**
     * Retrieves and and removes element from the head of this filequeue.
     *
     * @return element from the tail of the filequeue or null if filequeue is empty
     */
    public synchronized byte[] poll() {
        if (mvMap.isEmpty()) {
            tailKey.set(0);
            return null;
        }
        return mvMap.remove(mvMap.firstKey());
    }

    /**
     * Pushes element to the tail of this filequeue.
     *
     * @param element byte array containing element data
     */
    public synchronized void push(final byte[] element) {
        Preconditions.checkNotNull(element, "cant insert null");
        mvMap.put(tailKey.incrementAndGet(), element);
    }

    public void clear() {
        mvMap.clear();
    }

    /**
     * Returns the size of this filequeue.
     *
     * @return the size of the filequeue
     */
    public long size() {
        return mvMap.size();
    }

    /**
     * Determines if this filequeue is empty (equivalent to <code>{@link #size()} == 0</code>).
     *
     * @return <code>true</code> if this filequeue is empty, otherwise <code>false</code>.
     */
    public boolean isEmpty() {
        return mvMap.isEmpty();
    }

    /**
     * Closes this filequeue and frees up all resources associated to it.
     */
    public synchronized void close() {
        store.sync();
        store.close();
    }

    @Override
    public int compareTo(@Nonnull MVStoreQueue o) {
        int result = (int) (this.size() - o.size());
        return Integer.compare(result, 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MVStoreQueue)) return false;

        MVStoreQueue that = (MVStoreQueue) o;

        if (!queueName.equals(that.queueName)) return false;
        if (!store.equals(that.store)) return false;
        return getQueueDir().equals(that.getQueueDir());
    }

    @Override
    public int hashCode() {
        int result = queueName.hashCode();
        result = 31 * result + store.hashCode();
        result = 31 * result + getQueueDir().hashCode();
        return result;
    }

    public void commit() {
        store.commit();
    }

    private static class JimFSDecorator extends FilePath {

        private Path decorated;

        public JimFSDecorator() {
        }

        private JimFSDecorator(Path path) {
            this.name = path.toString();
            this.decorated = path;
        }

        @Override
        public long size() {
            try {
                return Files.size(decorated);
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public void moveTo(FilePath filePath, boolean b) {

            System.out.println(name);
            System.out.println(filePath);
        }

        @Override
        public boolean createFile() {
            try {
                Files.createFile(decorated);
                return Files.exists(decorated);
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public boolean exists() {
            return Files.exists(decorated);
        }

        @Override
        public void delete() {
            try {
                Files.deleteIfExists(decorated);
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public List<FilePath> newDirectoryStream() {
            try(Stream<Path> pathStream = Files.walk(decorated)) {
                return pathStream.map(JimFSDecorator::new).collect(Collectors.toList());
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public FilePath toRealPath() {
            return getPath(decorated);
        }

        @Override
        public FilePath getParent() {
            Path parent = decorated.getParent();
            return parent == null ? null : getPath(parent);
        }

        @Override
        public boolean isDirectory() {
            return Files.isDirectory(decorated);
        }

        @Override
        public boolean isAbsolute() {
            return decorated.toFile().isAbsolute();
        }

        @Override
        public long lastModified() {
            try {
                return Files.getLastModifiedTime(decorated).toMillis();
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public boolean canWrite() {
            return Files.isWritable(decorated);
        }

        @Override
        public void createDirectory() {
            try {
                Files.createDirectories(decorated);
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public OutputStream newOutputStream(boolean append) throws IOException {
            return append ? Files.newOutputStream(decorated, StandardOpenOption.APPEND)
                    : Files.newOutputStream(decorated);
        }

        @Override
        public FileChannel open(String s) throws IOException {
            try {
                Files.createFile(decorated);
            } catch (FileAlreadyExistsException exist) {

            }

            if ("r".equalsIgnoreCase(s)) return FileChannel.open(decorated, StandardOpenOption.READ);
            return FileChannel.open(decorated, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        @Override
        public InputStream newInputStream() throws IOException {
            return Files.newInputStream(decorated);
        }

        @Override
        public boolean setReadOnly() {
            return decorated.toFile().setReadOnly();
        }

        @Override
        public String getScheme() {
            return "jimfs";
        }

        @Override
        public FilePath getPath(String path) {
            try {
                URI uri = new URI(path);
                if (!uri.getScheme().equalsIgnoreCase(getScheme()))
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, uri.getScheme() + " not exists");
                return new JimFSDecorator(Paths.get(uri));
            } catch (URISyntaxException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        private FilePath getPath(Path path) {
            return new JimFSDecorator(path);
        }

    }
}