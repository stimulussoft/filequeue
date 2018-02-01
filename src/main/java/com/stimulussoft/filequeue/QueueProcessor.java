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
 */

package com.stimulussoft.filequeue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;
import com.stimulussoft.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * Queue processor. This class is thread-safe.
 *
 * @author Valentin Popov
 * @author Jamie Band
 * Thanks for Martin Grotze for his original work on Persistent Queue
 */


class QueueProcessor<T> {

    private static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);
    private final static long SECOND_MILLIS = 1000;
    private static final ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors() * 8, 60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(true),
            ThreadUtil.getFlexibleThreadFactory("filequeue-worker", false),
            new DelayRejectPolicy());
    private static final ScheduledExecutorService mvstoreCleanUP = Executors.newSingleThreadScheduledExecutor(
            ThreadUtil.getFlexibleThreadFactory("mvstore-cleanup", false));

    static {
        MoreExecutors.addDelayedShutdownHook(executorService, 60L, TimeUnit.SECONDS);
        MoreExecutors.addDelayedShutdownHook(mvstoreCleanUP, 60L, TimeUnit.SECONDS);
    }

    private final ObjectMapper objectMapper;
    private final MVStoreQueue mvStoreQueue;
    private final Class<T> type;
    private final Consumer<T> consumer;
    private final Expiration<T> expiration;
    private final Phaser restorePolled = new Phaser();
    private Optional<ScheduledFuture<?>> cleanupTask = Optional.empty();
    private volatile boolean doRun = true;
    private int maxTries = 0;
    private int retryDelaySecs = 0;

    /**
     * Create a new QueueProcessor
     *
     * @param queuePath      path to queue database
     * @param queueName      friendly name for the queue
     * @param type           filequeueitem type
     * @param maxTries       maximum number of retries
     * @param retryDelaySecs retry delays in secs
     * @param consumer       queue consumer
     * @param expiration     notification for item expiry
     * @throws IllegalStateException    if the queue is not running
     * @throws IllegalArgumentException if the type cannot be serialized by jackson
     * @throws IOException              if the item could not be serialized
     */

    QueueProcessor(final Path queuePath, final String queueName, final Class<T> type, int maxTries,
                          int retryDelaySecs, Consumer<T> consumer, Expiration<T> expiration) throws IOException, IllegalStateException, IllegalArgumentException {
        objectMapper = createObjectMapper();
        if (!objectMapper.canSerialize(type)) {
            throw new IllegalArgumentException("The given type cannot be serialized by jackson " +
                    "(checked with new ObjectMapper().canSerialize(type)).");
        }
        mvStoreQueue = new MVStoreQueue(queuePath, queueName);
        this.consumer = consumer;
        this.expiration = expiration;
        this.type = type;
        this.maxTries = maxTries;
        this.retryDelaySecs = retryDelaySecs;
        cleanupTask = Optional.of(mvstoreCleanUP.scheduleWithFixedDelay(new MVStoreCleaner(this), 5, 1, TimeUnit.MINUTES));
    }

    /**
     * Create a new QueueProcessor
     *
     * @param queuePath      path to queue database
     * @param queueName      friendly name for the queue
     * @param type           filequeueitem type
     * @param maxTries       maximum number of retries
     * @param retryDelaySecs retry delays in secs
     * @param consumer       queue consumer
     * @throws IllegalStateException    if the queue is not running
     * @throws IllegalArgumentException if the type cannot be serialized by jackson
     * @throws IOException              if the item could not be serialized
     */
    QueueProcessor(final Path queuePath, final String queueName, final Class<T> type, int maxTries,
                          int retryDelaySecs, Consumer<T> consumer) throws IOException, IllegalStateException, IllegalArgumentException {

        this(queuePath, queueName, type, maxTries, retryDelaySecs, consumer, null);
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

    public Path getQueueBaseDir() {
        return mvStoreQueue.getQueueDir();
    }

    public void reopen() throws IllegalStateException {
        mvStoreQueue.reopen();
    }

    /**
     * Submit item for instant processing with embedded pool. If item can't be processed instant
     * it will be queued on filesystem and processed after.
     *
     * @param item queue item
     * @throws IllegalStateException if the queue is not running
     * @throws IOException           if the item could not be serialized
     */

    public void submit(final T item) throws IllegalStateException, IOException {
        if (!doRun)
            throw new IllegalStateException("file queue {" + getQueueBaseDir() + "} is not running");
        try {
            restorePolled.register();
            executorService.execute(new ProcessItem<>(consumer, expiration, item, this));
        } catch (RejectedExecutionException | CancellationException cancel) {
            mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
        } finally {
            restorePolled.arriveAndDeregister();
        }
    }

    public void close() {
        doRun = false;
        cleanupTask.ifPresent(cleanupTask -> cleanupTask.cancel(true));
        restorePolled.register();
        restorePolled.arriveAndAwaitAdvance();
        mvStoreQueue.close();
    }

    public long size() {
        return mvStoreQueue.size();
    }

    private void retry(T item) throws IOException {
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

    private class ProcessItem<T> implements Runnable {

        private final Consumer<T> consumer;
        private final Expiration<T> expiration;
        private final T item;
        private final QueueProcessor<T> queueProcessor;

        ProcessItem(Consumer<T> consumer, Expiration<T> expiration, T item, QueueProcessor<T> queueProcessor) {
            this.consumer = consumer;
            this.expiration = expiration;
            this.item = item;
            this.queueProcessor = queueProcessor;
        }

        @Override
        public void run() {
            try {
                if (!consumer.consume(item)) {
                    if (queueProcessor.isNeedRetry(item)) {
                        if (queueProcessor.isTimeToRetry(item))
                            queueProcessor.retry(item);
                        else
                            mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
                    } else {
                        if (expiration != null)
                            expiration.expire(item);
                    }
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

    private final class MVStoreCleaner implements Runnable {

        private final QueueProcessor processingQueue;

        MVStoreCleaner(QueueProcessor processingQueue) {
            this.processingQueue = processingQueue;
        }

        @Override
        public void run() {
            if (doRun && !mvStoreQueue.isEmpty()) {
                try {
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
                        } catch (IllegalStateException e) {
                            logger.error("Failed to process item.", e);
                            mvStoreQueue.push(toDeserialize);
                        } finally {
                            restorePolled.arriveAndDeregister();
                        }
                    }
                } catch (IOException io) {
                    logger.error("Failed to process item.", io);
                }
            }
        }

    }

}