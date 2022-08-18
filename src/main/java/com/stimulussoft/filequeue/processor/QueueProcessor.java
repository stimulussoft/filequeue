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

package com.stimulussoft.filequeue.processor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;
import com.stimulussoft.filequeue.FileQueueItem;
import com.stimulussoft.filequeue.store.MVStoreQueue;
import com.stimulussoft.util.AdjustableSemaphore;
import com.stimulussoft.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/**
 * Queue processor. This class is for internal use only. Please refer to FileQueue class.
 *
 * @author Valentin Popov
 * @author Jamie Band
 * Thanks for Martin Grotze for his original work on Persistent Queue
 */


public class QueueProcessor<T> {

    private ExecutorService executorService;
    private static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);
    private static final ScheduledExecutorService mvstoreCleanUPScheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),
            ThreadUtil.getFlexibleThreadFactory("mvstore-cleanup", true));

    static {
        MoreExecutors.addDelayedShutdownHook(mvstoreCleanUPScheduler, 60L, TimeUnit.SECONDS);

    }

    public static void destroy() {
        MoreExecutors.shutdownAndAwaitTermination(mvstoreCleanUPScheduler, 60L, TimeUnit.SECONDS);
    }

    public enum RetryDelayAlgorithm {FIXED, EXPONENTIAL}

    private final ObjectMapper objectMapper;
    private final MVStoreQueue mvStoreQueue;
    private final Type type;
    private final Consumer<T> consumer;
    private final Expiration<T> expiration;
    private final Phaser restorePolled = new Phaser();
    private Optional<ScheduledFuture<?>> cleanupTaskScheduler;
    private volatile boolean doRun = true;
    private final int maxTries;

    private int maxQueueSize;
    private final int retryDelay;
    private final int persistRetryDelay;
    private final int maxRetryDelay;
    private final Path queuePath;
    private final String queueName;
    private final TimeUnit retryDelayUnit;
    private final TimeUnit persistRetryDelayUnit;
    private final RetryDelayAlgorithm retryDelayAlgorithm;
    private final AdjustableSemaphore permits = new AdjustableSemaphore();

    /**
     * Create a new QueueProcessor
     *
     * @param builder queue processor builder
     * @throws IllegalStateException    if the queue is not running
     * @throws IllegalArgumentException if the type cannot be serialized by jackson
     * @throws IOException              if the item could not be serialized
     */

    QueueProcessor(QueueProcessorBuilder<T> builder) throws IOException, IllegalStateException, IllegalArgumentException, InterruptedException {
        if (builder.queueName == null) throw new IllegalArgumentException("queue name must be specified");
        if (builder.queuePath == null) throw new IllegalArgumentException("queue path must be specified");
        if (builder.type == null) throw new IllegalArgumentException("item type must be specified");
        if (builder.consumer == null) throw new IllegalArgumentException("consumer must be specified");
        objectMapper = createObjectMapper();
        if (!objectMapper.canSerialize(objectMapper.constructType(builder.type).getClass()))
            throw new IllegalArgumentException("The given type is not serializable. it cannot be serialized by jackson");
        this.queueName = builder.queueName;
        this.queuePath = builder.queuePath;
        this.consumer = builder.consumer;
        this.executorService = builder.executorService;
        this.expiration = builder.expiration;
        this.type = builder.type;
        this.maxTries = builder.maxTries;
        this.retryDelay = builder.retryDelay;
        this.retryDelayUnit = builder.retryDelayUnit;
        this.maxRetryDelay = builder.maxRetryDelay;
        this.maxQueueSize = builder.maxQueueSize;
        this.retryDelayAlgorithm = builder.retryDelayAlgorithm;
        mvStoreQueue = new MVStoreQueue(builder.queuePath, builder.queueName);
        if (builder.persistRetryDelay <= 0)
            this.persistRetryDelay = retryDelay <= 1 ? 1 : retryDelay / 2;
        else
            this.persistRetryDelay = builder.persistRetryDelay;
        this.persistRetryDelayUnit = builder.persistRetryDelayUnit;
        cleanupTaskScheduler = Optional.of(mvstoreCleanUPScheduler.scheduleWithFixedDelay(new MVStoreCleaner(this), 0, persistRetryDelay, persistRetryDelayUnit));
        setMaxQueueSize(builder.maxQueueSize);
    }

    /**
     * Get a diff between two dates
     *
     * @param date1 the oldest date
     * @param date2 the newest date
     * @param unit  the unit in which you want the diff
     * @return the diff value, in the provided unit
     */
    private static long dateDiff(Date date1, Date date2, TimeUnit unit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return unit.convert(diffInMillies, TimeUnit.MILLISECONDS);
    }


    public Path getQueueBaseDir() {
        return mvStoreQueue.getQueueDir();
    }

    public void reopen() throws IllegalStateException {
        mvStoreQueue.reopen();
    }


    public int availablePermits() {
        return permits.availablePermits();
    }

    /**
     * Submit item for instant processing with embedded pool. If item can't be processed instant
     * it will be queued on filesystem and processed after.
     *
     * @param item            queue item
     * @param acquireWait     block for x msec
     * @param acquireWaitUnit wait block for time unit
     * @throws IllegalStateException if the queue is not running
     * @throws IOException           if the item could not be serialized
     */

    public void submit(final T item, int acquireWait, TimeUnit acquireWaitUnit) throws IllegalStateException, IOException, InterruptedException {
        if (!doRun)
            throw new IllegalStateException("file queue {" + getQueueBaseDir() + "} is not running");
        if (!permits.tryAcquire(1, acquireWait, acquireWaitUnit))
            throw new IOException("filequeue " + queuePath + " is full. {maxQueueSize='" + maxQueueSize + "'}");
        _submit(item);
    }

    /**
     * Submit item for instant processing with embedded pool. If item can't be processed instant
     * it will be queued on filesystem and processed after.
     *
     * @param item queue item
     * @throws IllegalStateException if the queue is not running
     * @throws IOException           if the item could not be serialized
     */

    public void submit(final T item) throws IllegalStateException, IOException, InterruptedException {
        if (!doRun)
            throw new IllegalStateException("file queue {" + getQueueBaseDir() + "} is not running");
        permits.acquire(1);
        _submit(item);
    }


    private void _submit(final T item) throws IllegalStateException, IOException {
        try {
            restorePolled.register();
            executorService.execute(new ProcessItem<>(consumer, expiration, item, this));
        } catch (RejectedExecutionException | CancellationException cancel) {
            try {
                mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
            } catch (Throwable t) {
                permits.release();
                throw t;
            }
        } finally {
            restorePolled.arriveAndDeregister();
        }
    }


    public void close() {
        doRun = false;
        cleanupTaskScheduler.ifPresent(cleanupTask -> cleanupTask.cancel(true));
        restorePolled.register();
        restorePolled.arriveAndAwaitAdvance();
        mvStoreQueue.close();
        permits.release(permits.drainPermits());
    }

    public void setMaxQueueSize(int maxQueueSize) throws InterruptedException {
        this.maxQueueSize = maxQueueSize;
        permits.release(permits.drainPermits());
        permits.setMaxPermits(this.maxQueueSize);
        permits.acquire((int) mvStoreQueue.size() > this.maxQueueSize ? this.maxQueueSize : (int) mvStoreQueue.size());
    }

    public long size() {
        return mvStoreQueue.size();
    }

    private void tryItem(T item) {
        ((FileQueueItem) item).setTryDate(new Date());
        ((FileQueueItem) item).incTryCount();
        //  System.out.println("try count "+((FileQueueItem) item).getTryCount());
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
        if (maxTries <= 0) return true;
        FileQueueItem queueItem = (FileQueueItem) item;
        return queueItem.getTryCount() < maxTries;
    }

    private boolean isTimeToRetry(T item) {
        switch (retryDelayAlgorithm) {
            case EXPONENTIAL:
                long tryDelay = Math.round(Math.pow(2, ((FileQueueItem) item).getTryCount()));
                tryDelay = tryDelay > maxRetryDelay ? maxRetryDelay : tryDelay;
                tryDelay = tryDelay < retryDelay ? retryDelay : tryDelay;
                return isTimeToRetry(item, tryDelay, retryDelayUnit);
            default:
                return isTimeToRetry(item, retryDelay, retryDelayUnit);
        }
    }

    private boolean isTimeToRetry(T item, long retryDelay, TimeUnit timeUnit) {
        return ((FileQueueItem) item).getTryDate() == null || dateDiff(((FileQueueItem) item).getTryDate(), new Date(), timeUnit) > retryDelay;
    }

    private T deserialize(final byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, objectMapper.constructType(type));
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

        private boolean pushBack() {
            if (isPushBack()) {
                try {
                    mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
                    return true;
                } catch (Throwable e1) {
                    logger.error("failed to process item {" + item.toString() + "}", e1);
                }
            }
            return false;
        }

        private void flagPush() {
            pushback = true;
        }

        private boolean isPushBack() {
            return pushback;
        }

        @Override
        public void run() {
            try {
                queueProcessor.tryItem(item);
                if (consumer.consume(item) == Consumer.Result.FAIL_REQUEUE)
                    flagPush();
            } catch (InterruptedException e) {
                flagPush();
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                logger.error("failed to process item {" + item.toString() + "}", e);
            } finally {
                if (!pushBack())
                    permits.release();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null) return false;
            if (!(o instanceof ProcessItem)) return false;
            ProcessItem<?> that = (ProcessItem) o;

            return Objects.equals(item.toString(), that.item.toString());
        }

        @Override
        public int hashCode() {
            return item != null ? item.toString().hashCode() : 0;
        }
    }


    private final class MVStoreCleaner implements Runnable {

        private final QueueProcessor<T> queueProcessor;

        MVStoreCleaner(QueueProcessor<T> queueProcessor) {
            this.queueProcessor = queueProcessor;
        }

        @Override
        public void run() {
            byte[] pushBack = null;
            if (doRun && !mvStoreQueue.isEmpty()) {
                try {
                    byte[] toDeserialize;
                    while ((toDeserialize = mvStoreQueue.poll()) != null) {
                        restorePolled.register();
                        try {
                            if (!doRun || Arrays.equals(toDeserialize, pushBack)) {
                                mvStoreQueue.push(toDeserialize);
                                break;
                            }
                            final T item = deserialize(toDeserialize);
                            if (item == null) continue;
                            if (isNeedRetry(item)) {
                                if (isTimeToRetry(item))
                                    queueProcessor._submit(item);
                                else {
                                    mvStoreQueue.push(toDeserialize);
                                    if (pushBack == null)
                                        pushBack = toDeserialize;
                                }
                            } else {
                                if (expiration != null)
                                    expiration.expire(item);
                            }
                        } catch (IllegalStateException e) {
                            logger.error("Failed to process item.", e);
                            mvStoreQueue.push(toDeserialize);
                            if (pushBack == null)
                                pushBack = toDeserialize;
                        } finally {
                            restorePolled.arriveAndDeregister();
                        }
                    }
                } catch (Exception io) {
                    logger.error("Failed to process item.", io);
                } finally {
                    mvStoreQueue.commit();
                }
            }
        }
    }

    /**
     * Get queue path
     *
     * @return queue path
     */
    public Path getQueuePath() {
        return queuePath;
    }

    /**
     * Get queue name
     *
     * @return queue name
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Get retry delay consumer
     *
     * @return retry delay consumer
     */

    public Consumer getConsumer() {
        return consumer;
    }

    /**
     * Get queue item type
     *
     * @return type
     */
    public Type getType() {
        return type;
    }

    /**
     * Maximum number of tries. Set to zero for infinite.
     *
     * @return maximum number of retries
     */
    public int getMaxTries() {
        return maxTries;
    }

    /**
     * Get fixed delay in retryDelayUnit between retries
     *
     * @return delay between retries
     */
    public int getRetryDelay() {
        return retryDelay;
    }

    /**
     * Get maximum delay in retryDelayUnit between retries assuming exponential backoff enabled
     *
     * @return maximum delay between retries
     */
    public int getMaxRetryDelay() {
        return maxRetryDelay;
    }

    /**
     * Get retry delay time unit
     *
     * @return retry delay time unit
     */
    public TimeUnit getRetryDelayUnit() {
        return retryDelayUnit;
    }

    /**
     * Get retry delay algorithm (FIXED or EXPONENTIAL)
     *
     * @return either fixed or exponential backoff
     */
    public RetryDelayAlgorithm getRetryDelayAlgorithm() {
        return retryDelayAlgorithm;
    }

    /**
     * Get retry delay expiration
     *
     * @return retry delay expiration
     */
    public Expiration getExpiration() {
        return expiration;
    }

    /**
     * Get delay between processing items in queue database (on disk).
     *
     * @return persistent retry delay
     */
    public int getPersistRetryDelay() {
        return persistRetryDelay;
    }

    /**
     * Get persistent retry delay time unit
     *
     * @return cleanup delay time unit
     */
    public TimeUnit getPersistRetryDelayUnit() {
        return retryDelayUnit;
    }

}