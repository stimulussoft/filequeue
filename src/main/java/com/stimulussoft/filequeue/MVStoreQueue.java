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
 * @author Valentin Popov (performance refactorings)
 * @author Jamie Band (adaptation to MVStore, added multithreaded consumer support & retry delay option)
 * Thanks for Martin Grotze for his original work on Persistent Queue
 *
 * Fast queue implementation on top of MVStore. This class is thread-safe.
 */

package com.stimulussoft.filequeue;


import com.google.common.base.Preconditions;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

class MVStoreQueue implements Comparable<MVStoreQueue> {

    private final String queueName;
    private MVMap<Integer, byte[]> mvMap;
    private MVStore store;
    private String queueDir;
    private AtomicInteger tailKey = new AtomicInteger(0);

    /**
     * Creates instance of persistent filequeue.
     *
     * @param queueDir  filequeue database environment directory path
     * @param queueName descriptive filequeue name
     * @throws IOException thrown when the given queueEnvPath does not exist and cannot be created.
     */
    public MVStoreQueue(final Path queueDir,
                        final String queueName) throws IOException, FileQueueException {
        Files.createDirectories(queueDir);
        this.queueDir = queueDir.toAbsolutePath().toString();
        this.queueName = queueName;
        reopen();
    }

    /**
     * Creates instance of persistent filequeue in memory
     *
     * @param queueName descriptive filequeue name
     * @throws IOException thrown when the given queueEnvPath does not exist and cannot be created.
     */
    public MVStoreQueue(final String queueName) throws FileQueueException {
        this.queueDir = "nioMemFS:";
        this.queueName = queueName;
        reopen();
    }

    private String getDBName() {
        return Paths.get(queueDir, queueName).toString();
    }

    public synchronized void reopen() throws FileQueueException {
        try {
            if (store != null && !store.isClosed()) store.close();
        } catch (Exception ignored) {
        }

        try {
            store = getOpenStore();
            mvMap = store.openMap(queueName);
            if (!mvMap.isEmpty())
                tailKey.set(mvMap.lastKey());
        } catch (IllegalStateException e) {
            // filequeue database is corrupted, lets delete it and start over

            throw new FileQueueException(MessageFormat.format("Queue \"{0}\" database is corrupted", queueDir));

//    		FileUtil.deleteFile(new File(getDBName()));
//    		store = getOpenStore();
//            mvMap = store.openMap(queueName);
        }
    }

    private MVStore getOpenStore() {
        return new MVStore.Builder().fileName(getDBName()).cacheSize(1).open();
    }

    public Path getQueueDir() {
        return Paths.get(queueDir);
    }

    /**
     * Retrieves and and removes element from the head of this filequeue.
     *
     * @return element from the tail of the filequeue or null if filequeue is empty
     * @throws IOException in case of disk IO failure
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
     * @param {@link Nonnull} element
     * @throws IOException in case of disk IO failure
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
    public int compareTo(MVStoreQueue o) {
        int result = (int) (this.size() - o.size());
        return Integer.compare(result, 0);
    }
}