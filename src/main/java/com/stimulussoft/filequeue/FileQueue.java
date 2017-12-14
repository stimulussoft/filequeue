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

import com.stimulussoft.util.AdjustableSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * FileQueue is a fast and efficient persistent filequeue written in Java. FileQueue is backed by H2 Database MVStore.
 * To cater for situations where filequeue items could not be processed, it supports retry logic. As the filequeue is
 * persistent, if the program is quit and started again, it will begin where it left off. Refer to
 * <a href="https://github.com/stimulussoft/filequeue">filequeue github page</a> for more info.
 *
 *  1) Implement FileQueueItem or extend RetryQueueItem (for retry support)
 *  2) Implement a class that extends FileQueue
 *     a) implement getFileQueueClass to return class created in step 1) above
 *     b) implement processFileQueueItem(FileQueueItem item) to perform actual processing work
 *  3) Call init(..) to initialize the filequeue
 *  4) Call startQueue() to start the filequeue
 *  5) Call stopQueue() to stop the filequeue processing
 *
 * To see example, refer to com.stimulussoft.filequeue.FileQueueTest
 *
 * @author Jamie Band (Stimulus Software)
 * @author Valentin Popov (Stimulus Software)
 *
 */

public abstract class FileQueue {

    protected Logger logger = LoggerFactory.getLogger("com.stimulussoft.archiva");
    protected String queueName;
    protected ShutdownHook shutdownHook;
    protected AtomicBoolean isStarted = new AtomicBoolean();
    protected Path queuePath;
    protected int maxTries = 0;
    protected int tryDelaySecs = 0;
    protected int maxQueueSize = Integer.MAX_VALUE;
    protected AdjustableSemaphore permits = new AdjustableSemaphore();
    protected int minFreeSpaceMb = 20;
    protected int diskSpaceCheckDelayMsec = 20;
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
     * @param queueName name of the queue
     * @param queuePath location on disk where the queue database resides
     * @param maxQueueSize maximum no. items in the queue
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


    /* Result of filequeue work */

    public enum ProcessResult {
        PROCESS_SUCCESS, /* process was successful */
        PROCESS_FAIL_REQUEUE,  /* process failed, but must be requeued */
        PROCESS_FAIL_NOQUEUE /* process failed, don't requeue */
    }

    /**
     * set maximum filequeue size
     *
     * @param maxQueueSize maximum no items in the queue
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
     * @throws FileQueueException if processing was interrupted due to shutdown
     * @throws IOException if error reading the db
     */

    public synchronized void startQueue() throws FileQueueException, IOException {
        assert queueName != null;
        assert queuePath!=null;
        if (!isStarted.get()) {
            permits.setMaxPermits(maxQueueSize);
            initQueue();
            isStarted.set(true);
            if (shutdownHook == null)
                shutdownHook = new ShutdownHook();
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    /**
     * Initialize the queue
     */

    private void initQueue() throws IOException, FileQueueException {
        assert queuePath!=null;
        assert queueName!=null;
        Files.createDirectories(queuePath);
        transferQueue = new QueueProcessor<>(queuePath, queueName, getFileQueueItemClass(), maxTries,
                tryDelaySecs, fileQueueConsumer);
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

                if (shutdownHook != null) {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                }
            }
        }
    }

    /**
     * Queue item for delivery.
     *
     * @param fileQueueItem item for queuing
     * @param block whether to block if filequeue is full or throw an exception
     * @param acquireWait time to wait before checking if shutdown has occurred
     * @param acquireWaitUnit time unit for acquireWait above wait
     * @throws FileQueueException general filequeue error
     * @throws InterruptedException queuing was interrupted due to shutdown
     */

    public void queueItem(final FileQueueItem fileQueueItem, boolean block, int acquireWait, TimeUnit acquireWaitUnit) throws FileQueueException, InterruptedException {

        assert fileQueueItem != null;
        assert acquireWaitUnit != null;
        assert acquireWait >= 0;

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
     * @throws FileQueueException if a general exception occurred
     */

    public void queueItem(final FileQueueItem fileQueueItem) throws FileQueueException {

        assert fileQueueItem != null;
        assert isStarted.get();

        if (maxTries > 0 && !(fileQueueItem instanceof RetryQueueItem))
            throw new IllegalArgumentException("since max tries > 0, item must be subclasses retryqueueitem");

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
     * @return no items in the queue
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
     * @param queueName friendly name of queue
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
     * @throws FileQueueException if general exception occurred
     * @throws InterruptedException if waiting was interrupted due to shutdown
     */


    public void ready(boolean block, int acquireWait, TimeUnit acquireWaitUnit) throws FileQueueException, InterruptedException {
        
        assert acquireWaitUnit != null;
        assert acquireWait >=0;
        assert isStarted.get();

        File queuePathFile = queuePath.toFile();

        if (block) {
            boolean acquired = false;
            while (isStarted.get() && !acquired) {
                acquired = permits.tryAcquire(acquireWait, acquireWaitUnit);
            }

            long freeSpace = queuePathFile.getUsableSpace();
            if (freeSpace <= minFreeSpaceMb * 1024 * 1024) {
                logger.warn("not enough disk space on " + queuePath + " {freeSpace='" + freeSpace + "',minSpace='" + minFreeSpaceMb + "mb'}. blocking operations until diskspace is freed.");
            }
            while (isStarted.get() && freeSpace <= minFreeSpaceMb * 1024 * 1024) {
                freeSpace = queuePathFile.getUsableSpace();
                Thread.sleep(diskSpaceCheckDelayMsec);
            }
        } else {
            if (!permits.tryAcquire(acquireWait, acquireWaitUnit))
                throw new FileQueueException("filequeue " + queuePath + " is full. {maxQueueSize='" + maxQueueSize + "'}");
            long freeSpace = queuePathFile.getUsableSpace();

            if (freeSpace <= minFreeSpaceMb * 1024 * 1024) {
                throw new FileQueueException("not enough free space on " + queuePath + " {freeSpace='" + freeSpace + "',minSpace='" + minFreeSpaceMb + "mb'}");
            }
        }
    }

    /**
     * Set minimum free space to allow before a new item will be accepted on the queue
     * @param minFreeSpaceMb free space in MB
     */

    public void setMinFreeSpaceMb(int minFreeSpaceMb) {
        this.minFreeSpaceMb = minFreeSpaceMb;
    }

    /**
     * Return minimum free space to allow before a new item will be accepted on the queue
     * @return free space in MB
     */
    
    public int getMinFeeSpaceMb() {
        return minFreeSpaceMb;
    }
    
    /**
     * Return no items in filequeue
     * @return no queued items
     */

    /**
     * Set minimum free space to allow before a new item will be accepted on the queue
     * @param diskSpaceCheckDelayMsec diskspace check delay in msec
     */

    public void setDiskSpaceCheckDelayMsec(int diskSpaceCheckDelayMsec) {
        this.diskSpaceCheckDelayMsec = diskSpaceCheckDelayMsec;
    }

    /**
     * Return milliseconds to wait before checking the diskspace again
     * @return diskspace check delay in msec
     */

    public int getDiskSpaceCheckDelayMsec() {
        return diskSpaceCheckDelayMsec;
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

