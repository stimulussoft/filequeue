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

 * @author Valentin Popov (performance refactoring)
 * @author Jamie Band (adaptation to MVStore, added multithreaded consumer support & retry delay option)
 */

package com.stimulussoft.filequeue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;
import com.stimulussoft.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

class QueueProcessor<T> {

    protected static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);
    private final static long SECOND_MILLIS = 1000;
    private static final ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors() * 8, 60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(true),
            ThreadUtil.getFlexibleThreadFactory("filequeue-worker", false),
            new DelayRejectPolicy());
    private static final ScheduledExecutorService mvstoreCleanUP = Executors.newSingleThreadScheduledExecutor(
            ThreadUtil.getFlexibleThreadFactory("mvstore-cleanup", false));
    private static final PollStores pollStores = new PollStores();

    static {
        MoreExecutors.addDelayedShutdownHook(executorService, 60L, TimeUnit.SECONDS);
        MoreExecutors.addDelayedShutdownHook(mvstoreCleanUP, 60L, TimeUnit.SECONDS);
        mvstoreCleanUP.scheduleWithFixedDelay(pollStores, 5, 5, TimeUnit.SECONDS);
    }

    private final ObjectMapper objectMapper;
    private final MVStoreQueue mvStoreQueue;
    private final Class<T> type;
    private final Consumer<T> consumer;
    private final Phaser restorePolled = new Phaser();
    private volatile boolean doRun = true;
    private int maxTries = 0;
    private int retryDelaySecs = 0;

    public QueueProcessor(final Path queueROOT, final String queueName, final Class<T> type, int maxTries,
                          int retryDelaySecs, Consumer<T> consumer) throws IOException, FileQueueException {
        objectMapper = createObjectMapper();
        if (!objectMapper.canSerialize(type)) {
            throw new IllegalArgumentException("The given type cannot be serialized by jackson " +
                    "(checked with new ObjectMapper().canSerialize(type)).");
        }
        mvStoreQueue = new MVStoreQueue(queueROOT, queueName);
        this.consumer = consumer;
        this.type = type;
        this.maxTries = maxTries;
        this.retryDelaySecs = retryDelaySecs;
        pollStores.register(mvStoreQueue, new PollQueue(this));
    }

    /**
     * @param earlierDate
     * @param laterDate
     * @return Math.abs between 2 dates or 0 if any of param are null.
     */
    private static int secondDiff(Date earlierDate, Date laterDate) {
        if (earlierDate == null || laterDate == null) return 0;
        return (int) (Math.abs((laterDate.getTime() - earlierDate.getTime()) / SECOND_MILLIS));
    }

    public File getQueueBaseDir() {
        return mvStoreQueue.getQueueDir().toFile();
    }

    public void reopen() throws FileQueueException {
        mvStoreQueue.reopen();
    }

    /**
     * Submit item for instant processing with embedded pool. If item can't be processed instant
     * it will be queued on filesystem and processed after.
     *
     * @param item
     * @throws FileQueueException
     */
    public void submit(final T item) throws FileQueueException {
        if (!doRun)
            throw new FileQueueException("filequeue is not running");

        try {
            try {
                restorePolled.register();
                executorService.execute(new ProcessItem<>(consumer, item, this));
            } catch (RejectedExecutionException | CancellationException cancel) {
                mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
            } finally {
                restorePolled.arriveAndDeregister();
            }
        } catch (Exception io) {
            throw new FileQueueException("failed write to file store", io);
        }
    }

    public void close() {
        doRun = false;
        pollStores.unRegister(mvStoreQueue);
        restorePolled.register();
        restorePolled.arriveAndAwaitAdvance();
        mvStoreQueue.close();
    }

    public long size() {
        return mvStoreQueue.size();
    }

    private void retry(T item) throws FileQueueException, IOException {
        if (doRun)
            submit(item);
        else
            mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
    }

    /**
     * Create the {@link ObjectMapper} used for serializing.
     *
     * @return the configured {@link ObjectMapper}.
     */
    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }

    private boolean isNeedRetry(T item) {
        if (maxTries <= 0) return false;
            RetryQueueItem queueItem = (RetryQueueItem) item;
        return queueItem.getTryCount() < maxTries;
    }

    private boolean isTimeToRetry(T item) {
        if (maxTries <= 0) return false;
        Date tryDate = ((RetryQueueItem) item).getTryDate();
        Date newTryDate = new Date();
        if (tryDate == null ||
                secondDiff(((RetryQueueItem) item).getTryDate(), newTryDate) > retryDelaySecs) {
            ((RetryQueueItem) item).setTryDate(newTryDate);
            ((RetryQueueItem) item).incTryCount();
            return true;
        } else
            return false;
    }

    private T deserialize(final byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, type);
        } catch (IOException e) {
            logger.error("failed deserialize object {" + Arrays.toString(data) + "}", e);
            return null;
        }
    }

    private interface Poll extends Runnable, Comparable<MVStoreQueue> {

    }

    private final static class PollStores implements Runnable {

        private Map<MVStoreQueue, Poll> polls = Collections.synchronizedMap(new HashMap<>());

        public void register(MVStoreQueue mvStoreQueue, Poll poll) {
            polls.put(mvStoreQueue, poll);
        }

        public void unRegister(MVStoreQueue poll) {
            polls.remove(poll);
        }

        @Override
        public void run() {
            for (Poll poll : polls.values()) {
                poll.run();
            }
        }
    }

    private class ProcessItem<T> implements Runnable {

        private final Consumer<T> consumer;
        private final T item;
        private final QueueProcessor<T> processingQueue;

        public ProcessItem(Consumer<T> consumer, T item, QueueProcessor<T> processingQueue) {
            this.consumer = consumer;
            this.item = item;
            this.processingQueue = processingQueue;
        }

        @Override
        public void run() {
            try {
                if (!consumer.consume(item))
                    if (processingQueue.isNeedRetry(item)) {
                        if (processingQueue.isTimeToRetry(item))
                            processingQueue.retry(item);
                        else
                            mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
                    }
            } catch (InterruptedException e) {
                try {
                    mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
                } catch (Exception e1) {
                    logger.error("failed to process item {" + item.toString() + "}", e1);
                } finally {
                    Thread.currentThread().interrupt();
                }
            } catch (Exception e) {
                logger.error("failed to process item {" + item.toString() + "}", e);
            }
        }
    }


    private final class PollQueue implements Poll {

        private final QueueProcessor processingQueue;

        public PollQueue(QueueProcessor processingQueue) {
            this.processingQueue = processingQueue;
        }

        @Override
        public void run() {
            if (!mvStoreQueue.isEmpty()) {
                byte[] toDeserialize;
                while ((toDeserialize = mvStoreQueue.poll()) != null) {
                    restorePolled.register();
                    try {
                        if (!doRun) {
                            mvStoreQueue.push(toDeserialize);
                            break;
                        }

                        final T item = deserialize(toDeserialize);
                        if (item == null) continue;
                        processingQueue.submit(item);
                    } catch (FileQueueException e) {
                        logger.error("Failed to process item.", e);
                        mvStoreQueue.push(toDeserialize);
                    } finally {
                        restorePolled.arriveAndDeregister();
                    }
                }
            }
        }

        @Override
        public int compareTo(MVStoreQueue o) {
            return mvStoreQueue.compareTo(o);
        }
    }

}