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
import java.util.*;
import java.util.concurrent.*;

/**
 * Queue processor. This class is thread-safe.anupo
 *
 * @author Valentin Popov
 * @author Jamie Band
 * Thanks for Martin Grotze for his original work on Persistent Queue
 */


class QueueProcessor<T> {

    private static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);
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

    public enum RetryDelayAlgorithm { FIXED, EXPONENTIAL}

    private final ObjectMapper objectMapper;
    private final MVStoreQueue mvStoreQueue;
    private final Class<T> type;
    private final Consumer<T> consumer;
    private final Expiration<T> expiration;
    private final Phaser restorePolled = new Phaser();
    private Optional<ScheduledFuture<?>> cleanupTask;
    private volatile boolean doRun = true;
    private final int maxTries;
    private final int retryDelay;
    private final int maxRetryDelay;
    private final Path queuePath;
    private final String queueName;
    private final TimeUnit retryDelayTimeUnit;
    private final RetryDelayAlgorithm retryDelayAlgorithm;


    public static class Builder {

        private     Path queuePath;
        private     String queueName;
        private     Class type;
        private     int maxTries                = 0;
        private     int retryDelay              = 1;
        private     int maxRetryDelay           = 1;
        private     TimeUnit retryDelayTimeUnit = TimeUnit.SECONDS;
        private     Consumer consumer;
        private     Expiration expiration;
        private     RetryDelayAlgorithm retryDelayAlgorithm =  RetryDelayAlgorithm.FIXED;
        /**
         * Queue path
         * @param queuePath              path to queue database
         */
        public Builder queuePath(Path queuePath) { this.queuePath = queuePath; return this; }
        public Path getQueuePath() { return queuePath; }
        /**
         * Queue name
         * @param queueName              friendly name for the queue
         */
        public Builder queueName(String queueName) { this.queueName = queueName; return this; }
        public String getQueueName() { return queueName; }

        /**
         * Type of queue item
         * @param type                   filequeueitem type
         */
        public Builder type(Class type) { this.type = type; return this; }
        public Class getType() { return type; }

        /**
         * Maximum number of tries. Set to zero for infinite.
         * @param maxTries               maximum number of retries
         */
        public Builder maxTries(int maxTries) { this.maxTries = maxTries; return this; }
        public int getMaxTries() { return maxTries; }

        /**
         * Set fixed delay between retries
         * @param retryDelay             delay between retries
         */
        public Builder retryDelay(int retryDelay) { this.retryDelay = retryDelay; return this; }
        public int getRetryDelay() { return retryDelay; }

        /**
         * Set maximum delay between retries assuming exponential backoff enabled
         * @param maxRetryDelay            maximum delay between retries
         */
        public Builder maxRetryDelay(int maxRetryDelay) { this.maxRetryDelay = maxRetryDelay; return this; }
        public int getMaxRetryDelay() { return maxRetryDelay; }

        /**
         * Set retry delay time unit
         * @param retryDelayTimeUnit           retry delay time unit
         */
        public Builder retryDelayTimeUnit(TimeUnit retryDelayTimeUnit) { this.retryDelayTimeUnit = retryDelayTimeUnit; return this; }
        public TimeUnit getRetryDelayTimeUnit() { return retryDelayTimeUnit; }

        /**
         * Set retry delay algorithm (FIXED or EXPONENTIAL)
         * @param  retryDelayAlgorithm            set to either fixed or exponential backoff
         */
        public Builder retryDelayAlgorithm(RetryDelayAlgorithm retryDelayAlgorithm) { this.retryDelayAlgorithm = retryDelayAlgorithm; return this; }
        public RetryDelayAlgorithm getRetryDelayAlgorithm() { return retryDelayAlgorithm; }

        /**
         * Set retry delay consumer
         * @param  consumer            retry delay consumer
         */
        public Builder consumer(Consumer consumer) { this.consumer = consumer; return this; }
        public Consumer getConsumer() { return consumer; }

        /**
         * Set retry delay expiration
         * @param  expiration            retry delay expiration
         */
        public Builder expiration(Expiration expiration) { this.expiration = expiration; return this; }
        public Expiration getExpiration() { return expiration; }

        public QueueProcessor build() throws IOException, IllegalStateException, IllegalArgumentException {
            return new QueueProcessor(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a new QueueProcessor
     * @param builder                   queue processor builder
     * @throws IllegalStateException    if the queue is not running
     * @throws IllegalArgumentException if the type cannot be serialized by jackson
     * @throws IOException              if the item could not be serialized
     */

    QueueProcessor(Builder builder) throws IOException, IllegalStateException, IllegalArgumentException {
        assert builder.queueName != null : "queue name must be specified";
        assert builder.queuePath != null : "queue path must be specified";
        assert builder.type != null : "item type must be specified";
        assert builder.consumer != null : "consumer must be specified";
        objectMapper = createObjectMapper();
        assert objectMapper.canSerialize(builder.type) : "The given type is not serializable. it cannot be serialized by jackson";

        this.queueName = builder.queueName;
        this.queuePath = builder.queuePath;
        this.consumer = builder.consumer;
        this.expiration = builder.expiration;
        this.type = builder.type;
        this.maxTries = builder.maxTries;
        this.retryDelay = builder.retryDelay;
        this.retryDelayTimeUnit = builder.retryDelayTimeUnit;
        this.maxRetryDelay = builder.maxRetryDelay;
        this.retryDelayAlgorithm = builder.retryDelayAlgorithm;
        mvStoreQueue = new MVStoreQueue(builder.queuePath, builder.queueName);
        int cleanupDelay = retryDelay <= 1 ? 1 : retryDelay / 2;
        cleanupTask = Optional.of(mvstoreCleanUP.scheduleWithFixedDelay(new MVStoreCleaner(this), cleanupDelay, cleanupDelay, retryDelayTimeUnit));
    }

    /**
     * Get a diff between two dates
     * @param date1 the oldest date
     * @param date2 the newest date
     * @param timeUnit the unit in which you want the diff
     * @return the diff value, in the provided unit
     */
    private static long dateDiff(Date date1, Date date2, TimeUnit timeUnit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
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

    private void tryItem(T item) {
        if (maxTries > 0) {
            ((FileQueueItem) item).setTryDate(new Date());
            ((FileQueueItem) item).incTryCount();
        }
    }

    /**
     * Create the {@link ObjectMapper} used for serializing.
     * @return the configured {@link ObjectMapper}.
     */
    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }

    private boolean isNeedRetry(T item) {
        if (maxTries <= 0) return true;
        FileQueueItem queueItem = (FileQueueItem) item;
        return queueItem.getTryCount() <= maxTries;
    }

    private boolean isTimeToRetry(T item) {
        switch (retryDelayAlgorithm) {
            case EXPONENTIAL:
                int tryDelay = ((int) Math.round(Math.pow(2,  ((FileQueueItem) item).getTryCount())));
                tryDelay = tryDelay > maxRetryDelay ? maxRetryDelay : tryDelay;
                tryDelay = tryDelay < retryDelay ? retryDelay : tryDelay;
                return isTimeToRetry(item, tryDelay);
            default:  return isTimeToRetry(item, retryDelay);
        }
    }

    private boolean isTimeToRetry(T item, int retryDelay) {
        return ((FileQueueItem) item).getTryDate() == null  || dateDiff(((FileQueueItem) item).getTryDate(), new Date(), retryDelayTimeUnit) > retryDelay;
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
        private boolean pushback = false;

        ProcessItem(Consumer<T> consumer, Expiration<T> expiration, T item, QueueProcessor<T> queueProcessor) {
            this.consumer = consumer;
            this.expiration = expiration;
            this.item = item;
            this.queueProcessor = queueProcessor;
        }

        private void pushBackIfNeeded() {
            if (isPushBack()) {
                try {
                    mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
                } catch (Exception e1) {
                    logger.error("failed to process item {" + item.toString() + "}", e1);
                }
            }
        }

        private void flagPush() { pushback = true; }
        private boolean isPushBack() { return pushback; }

        @Override
        public void run() {
            try {
                queueProcessor.tryItem(item);
                if (!consumer.consume(item)) flagPush();
            } catch (InterruptedException e) {
                flagPush();
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("failed to process item {" + item.toString() + "}", e);
            } finally {
                pushBackIfNeeded();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null) return false;
            if (getClass() != o.getClass()) return false;
            ProcessItem p = (ProcessItem) o;
            return Objects.equals(item.toString(), p.item.toString());
        }

        @Override
        public int hashCode() {
            return item.toString().hashCode();
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

                            if (isNeedRetry(item)) {
                                if (isTimeToRetry(item))
                                    processingQueue.submit(item);
                                else
                                    mvStoreQueue.push(toDeserialize);
                            } else {
                                if (expiration != null)
                                    expiration.expire(item);
                            }
                        } catch (IllegalStateException e) {
                            logger.error("Failed to process item.", e);
                            mvStoreQueue.push(toDeserialize);
                        } finally {
                            restorePolled.arriveAndDeregister();
                        }
                    }
                } catch (Exception io) {
                    logger.error("Failed to process item.", io);
                }
            }
        }
    }

    /**
     * Get queue path
     * @return queue path
     */
    public Path getQueuePath() {return queuePath; }

    /**
     * Get queue name
     * @return queue name
     */
    public String getQueueName() { return queueName; }
    /**
     * Get retry delay consumer
     * @return retry delay consumer
     */

    public Consumer getConsumer() {return consumer; }

    /**
     * Get queue item type
     * @return type
     */
    public Class getType() { return type; }
    /**
     * Maximum number of tries. Set to zero for infinite.
     * @return maximum number of retries
     */
    public int getMaxTries() { return maxTries; }

    /**
     * Get fixed delay between retries
     * @return delay between retries
     */
    public int getRetryDelay() { return retryDelay; }

    /**
     * Get maximum delay between retries assuming exponential backoff enabled
     * @return maximum delay between retries
     */
    public int getMaxRetryDelay() { return maxRetryDelay; }

    /**
     * Get retry delay time unit
     * @return retry delay time unit
     */
    public TimeUnit getRetryDelayTimeUnit() { return retryDelayTimeUnit; }

    /**
     * Get retry delay algorithm (FIXED or EXPONENTIAL)
     * @return either fixed or exponential backoff
     */
    public RetryDelayAlgorithm getRetryDelayAlgorithm() { return retryDelayAlgorithm;}

    /**
     * Get retry delay expiration
     * @return retry delay expiration
     */
    public Expiration getExpiration() { return expiration; }




}