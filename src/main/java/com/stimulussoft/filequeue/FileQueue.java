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
 * @author Jamie Band (Stimulus Software)
 * @author Valentin Popov (Stimulus Software)
 */

package com.stimulussoft.filequeue;

import com.stimulussoft.util.AdjustableSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/*
 * FileQueue is a fast and efficient persistent filequeue written in Java. FileQueue is backed by H2 Database MVStore.
 * To cater for situations where filequeue items could not be processed, it supports retry logic. As the filequeue is
 * persistent, if the program is quit and started again, it will begin where it left off. Refer to
 * <a href="https://github.com/jamieb22/filequeue">filequeue github page</a> for more info.
 *
 *  1) Implement FileQueueItem or extend RetryQueueItem (for retry support)
 *  2) Implement a class that extends FileQueue
 *     a) implement getFileQueueClass to return class created in step 1) above
 *     b) implement processFileQueueItem(FileQueueItem item) to perform actual processing work
 *  3) Call init(..) to initialize the filequeue
 *  4) Call startQueue() to start the filequeue
 *  5) Call stopQueue() to stop the filequeue processing
 *
 * To see example, refer to FileQueueTest
 */

public abstract class FileQueue {

    protected static final String BLOB_EXTENSION = ".blob";
    protected Logger logger = LoggerFactory.getLogger("com.stimulussoft.archiva");
    protected String queueName;
    protected ShutdownHook shutdownHook;
    protected AtomicBoolean isStarted = new AtomicBoolean();
    protected AtomicBoolean isInitialized = new AtomicBoolean();
    protected Path queuePath;
    protected int maxTries = 0;
    protected int tryDelaySecs = 0;
    protected int maxQueueSize = Integer.MAX_VALUE;
    protected AdjustableSemaphore permits = new AdjustableSemaphore();
    private QueueProcessor<FileQueueItem> transferQueue;

    final Consumer<FileQueueItem> fileQueueConsumer = item -> {
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
            release();
        }
    };

    /**
     * Create @{@link FileQueue}.
     * This method do not initialize the queue. Call init(..) to initialize the queue.
     */

    public FileQueue() {
    }

    /**
     * Create and initialize a @{@link FileQueue} at queuePath with a maximum queue size.
     * This method initializes the queue, so no need to call init(..) afterwards
     *
     * @param queueName friendly name for the queue
     * @param queuePath path where the queue database resides
     * @param maxQueueSize max size of the queue
     */
    public FileQueue(String queueName, Path queuePath, int maxQueueSize) {
        init(queueName, queuePath, maxQueueSize);
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * initialize a @{@link FileQueue} at queuePath with a maximum queue size.
     * This method must be called if the filequeue class is initialized using the default constructor.
     *
     * @param queueName
     * @param queuePath
     * @param maxQueueSize
     */

    public void init(String queueName, Path queuePath, int maxQueueSize) {
        assert queueName != null;
        assert queuePath != null;
        assert maxQueueSize > 0;
        this.queueName = queueName;
        this.queuePath = queuePath;
        this.maxQueueSize = maxQueueSize;
        isStarted.set(false);
    }

    /**
     * Override this method to return a custom FileQueueItem.class.
     * The FileQueueItem class is a must be serializable using Jackson JSON.
     */

    public abstract Class getFileQueueItemClass();

    /**
     * Override this method for processing of filequeue items.
     * This method performs the work of processing an item in the queue.
     * It is called when there is a queue item available for processing.
     *
     * @param item item for queuing
     * @return process result
     */

    public abstract ProcessResult processFileQueueItem(FileQueueItem item) throws InterruptedException;


    /* Result of filequeue work */

    public enum ProcessResult {
        PROCESS_SUCCESS, /* process was successful */
        PROCESS_FAIL_REQUEUE,  /* process failed, but must be requeued */
        PROCESS_FAIL_NOQUEUE /* process failed, don't requeue */
    }

    /**
     * set maximum filequeue size
     *
     * @param maxQueueSize
     */

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
        permits.setMaxPermits(maxQueueSize);
    }

    public Path getQueuePath() {
        return queuePath;
    }

    /**
     * Start the queue engine
     */

    public void startQueue() throws FileQueueException {
        logger.debug("going to start filequeue... {isStarted='" + isStarted + "'}");
        assert queueName != null;
        assert queuePath != null;
        synchronized (this) {
            if (!isStarted.get()) {
                permits.setMaxPermits(maxQueueSize);
                logger.debug("startQueue {queueName='" + queueName + "'}");
                try {
                    initQueue();
                    isStarted.set(true);
                    if (shutdownHook == null)
                        shutdownHook = new ShutdownHook();
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                    Runtime.getRuntime().addShutdownHook(shutdownHook);
                } catch (Exception e) {
                    throw new FileQueueException("failed to start filequeue:" + e.getMessage(), e, logger);
                }
                logger.debug("filequeue isStarted {queueName='" + queueName + "'}");
            } else {
                logger.debug("filequeue processor already isStarted {queueName='" + queueName + "'}");
            }
        }
    }

    /**
     * Initialize the queue
     */

    private void initQueue() throws IOException, FileQueueException {
        Files.createDirectories(queuePath);
        transferQueue = new QueueProcessor<>(queuePath, queueName, getFileQueueItemClass(), maxTries,
                tryDelaySecs, fileQueueConsumer);
    }


    /**
     * Stop the queue. Call this method when the queue engine must be shutdown.
     */

    public void stopQueue() {
        if (isStarted.compareAndSet(true, false)) {
            logger.debug("stop filequeue processor {',queueName='" + queueName + "'}");
            try {
                transferQueue.close();
            } finally {
                permits.release(permits.drainPermits());

                if (shutdownHook != null) {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                }
            }
            logger.debug("filequeue processor stopped {queueName='" + queueName + "'}");
        } else {
            logger.debug("filequeue processor already stopped {writeQueue='queueName='" + queueName + "'}");
        }
    }

    /**
     * Queue item for delivery.
     *
     * @param fileQueueItem item for queuing
     * @param block whether to block if filequeue is full or throw an exception
     * @param acquireWait time to wait before checking if shutdown has occurred
     * @param acquireWaitUnit time unit for acquireWait above wait
     */

    public void queueItem(final FileQueueItem fileQueueItem, boolean block, int acquireWait, TimeUnit acquireWaitUnit) throws Exception {
        try {
            ready(block, acquireWait, acquireWaitUnit);
        } catch (Exception e) {
            throw new FileQueueException("filequeue " + queuePath + " full:" + e.getMessage() + " {maxQueueSize='" + maxQueueSize + "'}");
        }
        queueItem(fileQueueItem);
    }

    /**
     * Queue item for delivery (no blocking)
     *
     * @param fileQueueItem item for queuing
     */

    public void queueItem(final FileQueueItem fileQueueItem) throws FileQueueException {
        if (!isStarted.get()) {
            throw new FileQueueException("filequeue " + queuePath + " is not started yet. {maxQueueSize='" + maxQueueSize + "'}");
        }

        if (fileQueueItem == null) {
            throw new FileQueueException("precondition: attempt to insert null item to filequeue.");
        }

        if (maxTries > 0 && !(fileQueueItem instanceof RetryQueueItem)) {
            throw new FileQueueException ("since max tries > 0, item must be subclasses retryqueueitem");
        }

        try {
            transferQueue.submit(fileQueueItem);
            // mvstore throws a null ptr exception when out of disk space
            // first we check whether at least 50 MB available space, if so, we try to reopen filequeue and push item again
            // if failed, we rethrow nullpointerexception
        } catch (NullPointerException npe) {
            if (transferQueue.getQueueBaseDir().getUsableSpace() > 50 * 1024 * 1024) {
                try {
                    transferQueue.reopen();
                    transferQueue.submit(fileQueueItem);
                } catch (Exception e) {
                    throw npe;
                }
            } else {
                throw npe;
            }
        }
    }

    /**
     * Return filequeue size
     */

    public long getQueueSize() {
        if (transferQueue != null) {
            return transferQueue.size();
        }
        return 0;
    }

    /**
     * Return filequeue name
     * @return filequeue name
     */

    public String getName() {
        return queueName;
    }

    /**
     * Set filequeue name
     * @@param filequeue name
     */


    public void setName(String queueName) {
        this.queueName = queueName;
    }

    /**
     * Return max retries
     * @return max retries
     */

    public int getMaxTries() {
        return maxTries;
    }


    /**
     * Set maximum retries
     * @param maxTries set maximum retries
     */

    public void setMaxTries(int maxTries) {
        this.maxTries = maxTries;
    }

    /**
     * Set retry delay (in seconds)
     * @param tryDelaySecs delay in seconds
     */

    public void setTryDelaySecs(int tryDelaySecs) {
        this.tryDelaySecs = tryDelaySecs;
    }

    protected void release() {
        permits.release();
    }

    /**
     * Call this function to block until space is available in filequeue for processing.
     * @param block whether to block if filequeue is full or throw an exception
     * @param acquireWait time to wait before checking if shutdown has occurred
     * @param acquireWaitUnit time unit for acquireWait above wait
     */


    public void ready(boolean block, int acquireWait, TimeUnit acquireWaitUnit) throws Exception {
        if (!isStarted.get())
            throw new FileQueueException("filequeue " + queuePath + " is not started yet. {maxQueueSize='" + maxQueueSize + "'}");

        File queuePathFile = queuePath.toFile();

        int MIN_FREE_SPACE_MB = 200;
        if (block) {
            boolean acquired = false;
            while (isStarted.get() && !acquired) {
                try {
                    acquired = permits.tryAcquire(acquireWait, acquireWaitUnit);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new Exception(e);
                }
            }

            long freeSpace = queuePathFile.getUsableSpace();
            if (freeSpace <= MIN_FREE_SPACE_MB * 1024 * 1024) {
                logger.warn("not enough disk space on " + queuePath + " {freeSpace='" + freeSpace + "',minSpace='" + MIN_FREE_SPACE_MB + "mb'}. blocking operations until diskspace is freed.");
            }
            while (isStarted.get() && freeSpace <= MIN_FREE_SPACE_MB * 1024 * 1024) {
                freeSpace = queuePathFile.getUsableSpace();
                int DISKSPACE_CHECK_DELAY_MSEC = 1000;
                Thread.sleep(DISKSPACE_CHECK_DELAY_MSEC);
            }
        } else {
            if (!permits.tryAcquire(acquireWait, acquireWaitUnit))
                throw new FileQueueException("filequeue " + queuePath + " is full. {maxQueueSize='" + maxQueueSize + "'}");
            long freeSpace = queuePathFile.getUsableSpace();

            if (freeSpace <= MIN_FREE_SPACE_MB * 1024 * 1024) {
                throw new FileQueueException("not enough free space on " + queuePath + " {freeSpace='" + freeSpace + "',minSpace='" + MIN_FREE_SPACE_MB + "mb'}");
            }
        }
    }

    /**
     * Return no items in filequeue
     * @return no queued items
     */

    public long getNoQueueItems() {
        return transferQueue.size();
    }

    class ShutdownHook extends Thread {

        @Override
        public void run() {
            shutdownHook = null;
            stopQueue();
        }
    }


}

