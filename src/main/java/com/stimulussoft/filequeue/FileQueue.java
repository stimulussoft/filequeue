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
import com.google.common.annotations.VisibleForTesting;
import com.stimulussoft.util.AdjustableSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * FileQueue is a fast and efficient persistent filequeue written in Java. FileQueue is backed by H2 Database MVStore.
 * To cater for situations where filequeue items could not be processed, it supports retry logic. As the filequeue is
 * persistent, if the program is quit and started again, it will begin where it left off. Refer to
 * <a href="https://github.com/stimulussoft/filequeue">filequeue github page</a> for more info.
 * <p>
 * 1) Implement FileQueueItem or extend RetryQueueItem (for retry support)
 * 2) Implement a class that extends FileQueue
 * a) implement getFileQueueClass to return class created in step 1) above
 * b) implement processFileQueueItem(FileQueueItem item) to perform actual processing work
 * 3) Call config() to configure filequeue
 * 4) Call startQueue() to start the filequeue
 * 5) Call stopQueue() to stop the filequeue processing
 * <p>
 * To see example, refer to com.stimulussoft.filequeue.FileQueueTest
 *
 * @author Jamie Band (Stimulus Software)
 * @author Valentin Popov (Stimulus Software)
 */

public abstract class FileQueue {

    private static final long fiftyMegs = 50L * 1024L * 1024L;
    protected Logger logger = LoggerFactory.getLogger("com.stimulussoft.archiva");
    private ShutdownHook shutdownHook;
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private final AdjustableSemaphore permits = new AdjustableSemaphore();
    private int minFreeSpaceMb = 20;
    private int diskSpaceCheckDelayMsec = 20;
    private QueueProcessor<FileQueueItem> transferQueue;
    private Config config;
    private final Expiration<FileQueueItem> fileQueueExpiration = this::expiredItem;
    public enum RetryDelayAlgorithm { FIXED, EXPONENTIAL}

    private final Consumer<FileQueueItem> fileQueueConsumer = item -> {
        try {
            if (!isStarted.get())
                return false;
            ProcessResult result = processFileQueueItem(item);
            switch (result) {
                case PROCESS_FAIL_REQUEUE:
                    return false;
                default:
                    return true;
            }
        } finally {
            permits.release();
        }
    };

    /**
     * Create @{@link FileQueue}.
     */

    public FileQueue() {
    }


    /**
     * Override this method to return a custom FileQueueItem.class.
     * The FileQueueItem class is a must be serializable using Jackson JSON.
     *
     * @return filequeueitem class
     */

    public abstract Class getFileQueueItemClass();

    /**
     * Override this method for processing of filequeue items.
     * This method performs the work of processing an item in the queue.
     * It is called when there is a queue item available for processing.
     *
     * @param item item for queuing
     * @return process result
     * @throws InterruptedException if processing was interrupted due to shutdown
     */

    public abstract ProcessResult processFileQueueItem(FileQueueItem item) throws InterruptedException;

    /**
     * Override this method to receive notification when an item is expired.
     * @param item  expired item
     */

    public abstract void expiredItem(FileQueueItem item);

    /**
     * Start the queue engine
     * @param config queue configuration. call config() to setup file queue configuration.
     * @throws IOException if error reading the db
     */

    public synchronized void startQueue(Config config) throws IOException, IllegalStateException, IllegalArgumentException {
        assert !isStarted.get();
        this.config = config;
        transferQueue = config.consumer(fileQueueConsumer).expiration(fileQueueExpiration).builder.build();
        permits.setMaxPermits(config.maxQueueSize);
        isStarted.set(true);
        shutdownHook = new ShutdownHook();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /**
     * Get currently active configuration.
     * @return configuration
     */

    public Config getConfig() {
        assert isStarted.get();
        return config;
    }

    /**
     * Stop the queue. Call this method when the queue engine must be shutdown.
     */

    public synchronized void stopQueue() {
        if (isStarted.compareAndSet(true, false)) {
            try {
                transferQueue.close();
            } finally {
                permits.release(permits.drainPermits());
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            }
        }
    }

    public static class Config {

        private int maxQueueSize = Integer.MAX_VALUE;
        private QueueProcessor.Builder builder = QueueProcessor.builder();

        public Config(String queueName, Path queuePath, Class type) {
            builder = builder.type(type).queueName(queueName).queuePath(queuePath);
        }

        public Config() { }

        /**
         * Queue path
         * @param queuePath             path to queue database
         */
        
        public Config queuePath(Path queuePath) { builder = builder.queuePath(queuePath); return this; }
        public Path getQueuePath() { return builder.getQueuePath(); }

        /**
         * Queue name
         * @param queueName              friendly name for the queue
         */
        public  Config queueName(String queueName) { builder = builder.queueName(queueName); return this; }
        public String getQueueName() { return builder.getQueueName(); }

        /**
         * Type of queue item
         * @param type                   filequeueitem type
         */
        public Config type(Class type) {
            assert type != FileQueueItem.class && FileQueueItem.class.isAssignableFrom(type) : "type must be a descendant of filequeue";
            builder = builder.type(type); return this;
        }
        public Class getType() { return builder.getType(); }

        /**
         * Maximum number of tries. Set to zero for infinite.
         * @param maxTries               maximum number of retries
         */
        public  Config maxTries(int maxTries) {builder = builder.maxTries(maxTries); return this; }
        public int getMaxTries() { return builder.getMaxTries(); }

        /**
         * Set fixed delay between retries
         * @param retryDelay             delay between retries
         */
        public  Config retryDelay(int retryDelay) { builder = builder.retryDelay(retryDelay); return this; }
        public int getRetryDelay() { return builder.getRetryDelay(); }

        /**
         * Set retry delay between retries from items in database (on disk)
         * @param retryDelay             delay between retries
         */
        public  Config persistentRetryDelay(int retryDelay) { builder = builder.persistentRetryDelay(retryDelay); return this; }
        public int getPersistentRetryDelay() { return builder.getPersistentRetryDelay(); }

        /**
         * Set  persistent retry delay time unit
         * @param persistentRetryDelayTimeUnit  persistent retry delay timeunit
         */
        public  Config persistentRetryDelayTimeUnit(TimeUnit persistentRetryDelayTimeUnit) { builder = builder.persistentRetryDelayTimeUnit(persistentRetryDelayTimeUnit); return this; }
        public TimeUnit getPersistentRetryDelayTimeUnit() { return builder.getPersistentRetryDelayTimeUnit(); }

        /**
         * Set maximum delay between retries assuming exponential backoff enabled
         * @param maxRetryDelay            maximum delay between retries
         */
        public  Config maxRetryDelay(int maxRetryDelay) { builder = builder.maxRetryDelay(maxRetryDelay); return this; }
        public int getMaxRetryDelay() { return builder.getMaxRetryDelay(); }


        /**
         * Set retry delay time unit
         * @param retryDelayTimeUnit           retry delay time unit
         */
        public  Config retryDelayTimeUnit(TimeUnit retryDelayTimeUnit) { builder = builder.retryDelayTimeUnit(retryDelayTimeUnit); return this; }
        public TimeUnit getRetryDelayTimeUnit() { return builder.getRetryDelayTimeUnit(); }

        /**
         * Set retry delay algorithm (FIXED or EXPONENTIAL)
         * @param  retryDelayAlgorithm            set to either fixed or exponential backoff
         */
        public Config retryDelayAlgorithm(RetryDelayAlgorithm retryDelayAlgorithm) {builder = builder.retryDelayAlgorithm(QueueProcessor.RetryDelayAlgorithm.valueOf(retryDelayAlgorithm.name())); return this; }
        public RetryDelayAlgorithm getRetryDelayAlgorithm() { return RetryDelayAlgorithm.valueOf(builder.getRetryDelayAlgorithm().name()); }

        /**
         * Set retry delay consumer
         * @param  consumer            retry delay consumer
         */
        private  Config consumer(Consumer consumer) {  builder = builder.consumer(consumer); return this; }
        public Consumer getConsumer() { return builder.getConsumer(); }

        /**
         * Set retry delay expiration
         * @param  expiration            retry delay expiration
         */
        public  Config expiration(Expiration expiration) {builder = builder.expiration(expiration); return this; }
        public Expiration getExpiration() { return builder.getExpiration(); }

        /**
         * Set max queue size
         * @param  maxQueueSize            maximum size of queue
         */
        public  Config maxQueueSize(int maxQueueSize) { this.maxQueueSize = maxQueueSize; return this; }
        public int getMaxQueueSize() { return maxQueueSize; }
    }

    /**
     * Setup a file queue configuration for pass to startQueue()
     */

    public static  Config config(String queueName, Path queuePath, Class type) {
        return new Config(queueName,queuePath,type);
    }

    public static  Config config() {
        return new Config();
    }

    /**
     * Queue item for delivery.
     *
     * @param fileQueueItem   item for queuing
     * @param block           whether to block if filequeue is full or throw an exception
     * @param acquireWait     time to wait before checking if shutdown has occurred
     * @param acquireWaitUnit time unit for acquireWait above wait
     * @throws IOException   general filequeue error
     * @throws InterruptedException queuing was interrupted due to shutdown
     */

    public void queueItem(final FileQueueItem fileQueueItem, boolean block, int acquireWait, TimeUnit acquireWaitUnit) throws IOException, InterruptedException {

        assert fileQueueItem != null;
        assert acquireWaitUnit != null;
        assert acquireWait >= 0;

        ready(block, acquireWait, acquireWaitUnit);
        queueItem(fileQueueItem);
    }

    /**
     * Queue item for delivery (no blocking)
     *
     * @param fileQueueItem item for queuing
     * @throws IllegalArgumentException if the wrong arguments were supplied
     * @throws IOException if the item could not be serialized
     */
    @VisibleForTesting
    public void queueItem(final FileQueueItem fileQueueItem) throws IOException, IllegalArgumentException {

        assert fileQueueItem != null;
        assert isStarted.get();

        try {
            transferQueue.submit(fileQueueItem);
            // mvstore throws a null ptr exception when out of disk space
            // first we check whether at least 50 MB available space, if so, we try to reopen filequeue and push item again
            // if failed, we rethrow nullpointerexception
        } catch (NullPointerException npe) {
            if (Files.getFileStore(transferQueue.getQueueBaseDir()).getUsableSpace() > fiftyMegs) {
                try {
                    transferQueue.reopen();
                    transferQueue.submit(fileQueueItem);
                } catch (Exception e) {
                    permits.release();
                    throw npe;
                }
            } else {
                permits.release();
                throw npe;
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Return filequeue size
     *
     * @return no items in the queue
     */

    public long getQueueSize() {
        if (transferQueue != null) {
            return transferQueue.size();
        }
        return 0;
    }

    /**
     * Set max queue size
     *
     * @param queue size
     */

    public void setMaxQueueSize(int queueSize) {
        if (config != null)
            config = config.maxQueueSize(queueSize);
        permits.setMaxPermits(queueSize);
    }

    public void release() {
        permits.release();
    }

    /**
     * Call this function to block until space is available in filequeue for processing.
     *
     * @param block           whether to block if filequeue is full or throw an exception
     * @param acquireWait     time to wait before checking if shutdown has occurred
     * @param acquireWaitUnit time unit for acquireWait above wait
     * @throws InterruptedException thrown if waiting was interrupted due to shutdown
     * @throws IOException thrown if there is not enough free space or the queue is full
     */
    protected void ready(boolean block, int acquireWait, TimeUnit acquireWaitUnit) throws IOException, InterruptedException {

        assert acquireWaitUnit != null;
        assert acquireWait >= 0;
        assert isStarted.get();

        long minFreeSpace = (long) minFreeSpaceMb * 1024L * 1024L;

        if (block) {
            boolean acquired = false;
            while (isStarted.get() && !acquired) {
                acquired = permits.tryAcquire(acquireWait, acquireWaitUnit);
            }

            long freeSpace = Files.getFileStore(transferQueue.getQueuePath()).getUsableSpace();
            if (freeSpace <= minFreeSpace)
                logger.warn("not enough disk space on " + transferQueue.getQueuePath() + " {freeSpace='" + freeSpace + "',minSpace='" + minFreeSpaceMb + "mb'}. " +
                        "blocking operations until diskspace is freed.");

            while (isStarted.get() && freeSpace <= minFreeSpace) {
                freeSpace = Files.getFileStore(transferQueue.getQueuePath()).getUsableSpace();
                Thread.sleep(diskSpaceCheckDelayMsec);
            }

        } else {
            if (!permits.tryAcquire(acquireWait, acquireWaitUnit))
                throw new IOException("filequeue " + transferQueue.getQueuePath() + " is full. {maxQueueSize='" + config.maxQueueSize + "'}");

            long freeSpace = Files.getFileStore(transferQueue.getQueuePath()).getUsableSpace();
            if (freeSpace <= minFreeSpace) {
                permits.release();
                throw new IOException("not enough free space on " + transferQueue.getQueuePath() + " {freeSpace='" + freeSpace + "',minSpace='" + minFreeSpaceMb + "mb'}");
            }

        }
    }

    /**
     * Set minimum free space to allow before a new item will be accepted on the queue
     *
     * @param minFreeSpaceMb free space in MB
     */

    public void setMinFreeSpaceMb(int minFreeSpaceMb) {
        this.minFreeSpaceMb = minFreeSpaceMb;
    }

    /**
     * Return minimum free space to allow before a new item will be accepted on the queue
     *
     * @return free space in MB
     */

    public int getMinFeeSpaceMb() {
        return minFreeSpaceMb;
    }

    /**
     * Return milliseconds to wait before checking the diskspace again
     *
     * @return diskspace check delay in msec
     */

    public int getDiskSpaceCheckDelayMsec() {
        return diskSpaceCheckDelayMsec;
    }

    /**
     * Return no items in filequeue
     * @return no queued items
     */

    /**
     * Set minimum free space to allow before a new item will be accepted on the queue
     *
     * @param diskSpaceCheckDelayMsec diskspace check delay in msec
     */

    public void setDiskSpaceCheckDelayMsec(int diskSpaceCheckDelayMsec) {
        this.diskSpaceCheckDelayMsec = diskSpaceCheckDelayMsec;
    }

    /**
     * Return no items in filequeue
     *
     * @return no queued items
     */

    public long getNoQueueItems() {
        return transferQueue.size();
    }


    public enum ProcessResult {
        PROCESS_SUCCESS, /* process was successful */
        PROCESS_FAIL_REQUEUE,  /* process failed, but must be requeued */
        PROCESS_FAIL_NOQUEUE /* process failed, don't requeue */
    }

    class ShutdownHook extends Thread {

        @Override
        public void run() {
            shutdownHook = null;
            stopQueue();
        }
    }


}

