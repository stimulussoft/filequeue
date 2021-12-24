package com.stimulussoft.filequeue;

import com.google.common.collect.Maps;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.stimulussoft.filequeue.processor.Consumer;
import com.stimulussoft.filequeue.processor.DelayRejectPolicy;
import com.stimulussoft.filequeue.processor.Expiration;
import com.stimulussoft.util.ThreadUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/* File Queue Test
 * Demonstrates how to use the filequeue
 * @author Valentin Popov
 */

public class FileQueueTest {

    private static boolean block = true;
    private static final int ROUNDS = 1000;
    private static final int BLOCKS = 8;
    private static final int RETRIES = 10;
    private static final int MAXRETRYDELAY = 10;
    private static final int RETRYDELAY = 2;
    private static final int MAXQUEUESIZE = 100;
    private static final int HUGEQUEUESIZE = 100000;
    private static final int SMALLQUEUESIZE = 200;
    private static final int PERSISTENT_RETRY_DELAY_MSEC = 100;
    private static final TimeUnit RetryDelayTimeUnit = TimeUnit.MILLISECONDS;

    private static AtomicInteger processedTestWithoutRetries = new AtomicInteger(0);
    private static AtomicInteger producedTestWithoutRetries = new AtomicInteger(0);
    private static AtomicInteger processedTestWithRetries = new AtomicInteger(0);
    private static AtomicInteger producedTestWithRetries = new AtomicInteger(0);
    private static AtomicInteger processedTestWithThrowable = new AtomicInteger(0);
    private static AtomicInteger producedTestWithThrowable = new AtomicInteger(0);
    private static AtomicInteger processedTestWithWait = new AtomicInteger(0);
    private static AtomicInteger producedTestWithWait = new AtomicInteger(0);
    private static AtomicInteger producedTestWithBlockage = new AtomicInteger(0);
    private static Map<String,AtomicInteger> retryTestWithRetries = Maps.newConcurrentMap();
    private static AtomicInteger processedTestWithRetryAndExponentialDelay = new AtomicInteger(0);
    private static AtomicInteger processedTestWithExpiry = new AtomicInteger(0);
    private static AtomicInteger processedTestWithRetriesAndWait = new AtomicInteger(0);
    private static AtomicInteger processedTestPersist = new AtomicInteger(0);
    private static AtomicInteger processedTestWithBlockage = new AtomicInteger(0);
    private static AtomicInteger producedTestWithRetryAndExponentialDelay = new AtomicInteger(0);
    private static AtomicInteger producedTestWithExpiry = new AtomicInteger(0);
    private static AtomicInteger producedTestWithRetriesAndWait = new AtomicInteger(0);
    private static AtomicInteger availableSlotTestWithRetriesAndWait = new AtomicInteger(0);
    private static AtomicInteger producedTestPersist = new AtomicInteger(0);
    private static Map<String,AtomicInteger> retryTestWithRetryAndExponentialDelay = Maps.newConcurrentMap();
    private static Map<String,AtomicInteger> retryTestWithExpiry = Maps.newConcurrentMap();
    private static Map<String,AtomicInteger> retryTestWithRetriesAndWait = Maps.newConcurrentMap();
    private static AtomicInteger expireTestWithExpiry =  new AtomicInteger(0);


    private static final ThreadPoolExecutor executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors() * 2, 60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(true),
            ThreadUtil.getFlexibleThreadFactory("filequeue-worker", false),
            new DelayRejectPolicy());

    /* Test Without Retries */

    public final class ThreadPoolQueue extends ArrayBlockingQueue<Runnable> {

        public ThreadPoolQueue(int capacity) {
            super(capacity);
        }

        @Override
        public boolean offer(Runnable e) {
            try {
                put(e);
            } catch (InterruptedException e1) {
                return false;
            }
            return true;
        }

    }

    @Test
    public void testWithoutRetries() throws Exception {
        String queueName = "testWithoutRetries";
        Path db = setup("filequeue test without retries", queueName, producedTestWithoutRetries, processedTestWithoutRetries);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestConsumer(), executorService).maxQueueSize(MAXQUEUESIZE).persistRetryDelay(1);
        queue.startQueue(config);
        Assert.assertEquals(queue.getConfig().getQueueName(), queueName);
        Assert.assertEquals(queue.getConfig().getQueuePath(), db);
        Assert.assertEquals(queue.getConfig().getMaxQueueSize(), MAXQUEUESIZE);
        producedTestWithoutRetries.set(0);
        processedTestWithoutRetries.set(0);

        for (int i = 0; i < ROUNDS; i++) {
            producedTestWithoutRetries.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        done(queue, producedTestWithoutRetries, processedTestWithoutRetries, null, ROUNDS);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);

    }

     /* Test With Retries */

    @Test
    public void testWithRetries() throws Exception {
        String queueName = "testWithRetries";
        Path db = setup("filequeue test with retries", queueName, producedTestWithRetries, processedTestWithRetries);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestRetryConsumer2(), executorService).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                                  .retryDelay(RETRYDELAY).retryDelayUnit(RetryDelayTimeUnit).persistRetryDelay(1);
        queue.startQueue(config);
        Assert.assertEquals(queue.getConfig().getMaxTries(), RETRIES);
        Assert.assertEquals(queue.getConfig().getRetryDelay(), RETRYDELAY);
        Assert.assertEquals(queue.getConfig().getRetryDelayUnit(), RetryDelayTimeUnit);
        producedTestWithRetries.set(0);
        processedTestWithRetries.set(0);

        // we will use a thread pool here to test if queueItem() method is thread-safe.
        ExecutorService executor = Executors.newFixedThreadPool(6);
        for (int i = 0; i < ROUNDS; i++) {
            final int no = i;
            executor.execute(() -> {
                try { producedTestWithRetries.incrementAndGet();
                queue.queueItem(new TestFileQueueItem(no), 1, TimeUnit.HOURS); } catch (Exception e) { throw new RuntimeException(e.getMessage()); }
            });
        }
        executor.shutdown();;
        done(queue, producedTestWithRetries, processedTestWithRetries, retryTestWithRetries, ROUNDS);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }


    /* Test With Throwable */


    @Test
    public void testWithThrowable() throws Exception {
        String queueName = "testWithThrowable";
        Path db = setup("filequeue test with throwable", queueName, producedTestWithThrowable, processedTestWithThrowable);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestThrowableConsumer(), executorService).maxQueueSize(MAXQUEUESIZE).persistRetryDelay(1);
        queue.startQueue(config);
        Assert.assertEquals(queue.getConfig().getQueueName(), queueName);
        Assert.assertEquals(queue.getConfig().getQueuePath(), db);
        Assert.assertEquals(queue.getConfig().getMaxQueueSize(), MAXQUEUESIZE);
        producedTestWithThrowable.set(0);
        processedTestWithThrowable.set(0);

        for (int i = 0; i < ROUNDS; i++) {
            producedTestWithThrowable.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        done(queue, producedTestWithThrowable, processedTestWithThrowable, null, ROUNDS);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);

    }

    @Test
    public void testWithWait() throws Exception {
        String queueName = "testWithThrowable";
        Path db = setup("filequeue test with throwable", queueName, producedTestWithWait, processedTestWithWait);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestWithWaitConsumer(), executorService).maxQueueSize(HUGEQUEUESIZE).persistRetryDelay(1);
        queue.startQueue(config);
        Assert.assertEquals(queue.getConfig().getQueueName(), queueName);
        Assert.assertEquals(queue.getConfig().getQueuePath(), db);
        Assert.assertEquals(queue.getConfig().getMaxQueueSize(), HUGEQUEUESIZE);
        producedTestWithWait.set(0);
        processedTestWithWait.set(0);

        for (int i = 0; i <ROUNDS; i++) {
            producedTestWithWait.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        done(queue, producedTestWithWait, processedTestWithWait, null, ROUNDS);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);

    }


    /* Test Without Retry And Exponential Delay */
    
    @Test
    public void testWithRetryAndExponentialDelay() throws Exception {
        String queueName = "testWithRetryAndExponentialDelay";
        Path db = setup("filequeue test with retries and exponential delay", queueName, producedTestWithRetryAndExponentialDelay, processedTestWithRetryAndExponentialDelay);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestRetryConsumer3(), executorService).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                .retryDelay(RETRYDELAY).retryDelayUnit(RetryDelayTimeUnit)
                .retryDelayAlgorithm(FileQueue.RetryDelayAlgorithm.EXPONENTIAL).retryDelay(RETRYDELAY).maxRetryDelay(MAXRETRYDELAY);
        queue.startQueue(config);
        producedTestWithRetryAndExponentialDelay.set(0);
        processedTestWithRetryAndExponentialDelay.set(0);

        for (int i = 0; i < ROUNDS; i++) {
            producedTestWithRetryAndExponentialDelay.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        done(queue, producedTestWithRetryAndExponentialDelay, processedTestWithRetryAndExponentialDelay, retryTestWithRetryAndExponentialDelay, ROUNDS);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    /* Test With Expiry */
    
    @Test
    public void testWithExpiry() throws Exception {
        String queueName = "testWithExpiry";
        Path db = setup("filequeue test with expiry", queueName, producedTestWithExpiry, processedTestWithExpiry);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestExpireConsumer(), executorService).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                .retryDelay(RETRYDELAY).retryDelayUnit(RetryDelayTimeUnit).expiration(new TestExpiration())
                .retryDelayAlgorithm(FileQueue.RetryDelayAlgorithm.EXPONENTIAL).retryDelay(RETRYDELAY)
                .maxRetryDelay(MAXRETRYDELAY);
        queue.startQueue(config);
        producedTestWithExpiry.set(0);
        processedTestWithExpiry.set(0);

        for (int i = 0; i < ROUNDS; i++) {
            producedTestWithExpiry.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        done(queue, producedTestWithExpiry,expireTestWithExpiry, retryTestWithExpiry, ROUNDS);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    /* Test With Retries And Wait */
    
    @Test
    public void testWithRetriesAndWait() throws Exception {
        String queueName = "testWithRetriesAndWait";
        Path db = setup("filequeue test with retries and block", queueName, producedTestWithRetriesAndWait, processedTestWithRetriesAndWait);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestExpireConsumer2(), executorService).maxQueueSize(SMALLQUEUESIZE)
                .retryDelay(RETRYDELAY).retryDelayUnit(RetryDelayTimeUnit).expiration(new TestExpiration2())
                 .maxRetryDelay(MAXRETRYDELAY).persistRetryDelay(PERSISTENT_RETRY_DELAY_MSEC)
                .persistRetryDelayUnit(TimeUnit.MILLISECONDS);
        queue.startQueue(config);
        Assert.assertEquals(SMALLQUEUESIZE,queue.availablePermits());
        producedTestWithRetriesAndWait.set(0);
        processedTestWithRetriesAndWait.set(0);
        availableSlotTestWithRetriesAndWait.set(0);
        QueueCallbackTest queueCallbackTest = new QueueCallbackTest();

        for (int i = 0; i < ROUNDS / 10; i++) {
            for (int j = 0; j < 10; j++) {
                producedTestWithRetriesAndWait.incrementAndGet();
                queue.queueItem(new TestFileQueueItem(i * 10 + j), queueCallbackTest, 1, TimeUnit.HOURS);
            }
        }
        done(queue, producedTestWithRetriesAndWait, processedTestWithRetriesAndWait, retryTestWithRetriesAndWait, ROUNDS);
        System.out.println("available slots "+availableSlotTestWithRetriesAndWait.get());
        Assert.assertEquals(ROUNDS,availableSlotTestWithRetriesAndWait.get());
        System.out.println("available permits "+queue.availablePermits());
        Assert.assertEquals(SMALLQUEUESIZE,queue.availablePermits());
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    public class QueueCallbackTest implements QueueCallback<FileQueueItem> {

        @Override
        public void availableSlot(FileQueueItem fileQueueItem) {
            availableSlotTestWithRetriesAndWait.incrementAndGet();
        }
    }

    /* Test With Queue Persistence */

    @Test
    public void testPersist() throws Exception {
        String queueName = "testPersist";
        Path db = setup("filequeue test with persist", queueName, producedTestPersist, processedTestPersist);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestShutdownConsumer(), executorService).maxQueueSize(MAXQUEUESIZE)
                .retryDelay(RETRYDELAY).retryDelayUnit(RetryDelayTimeUnit).expiration(new TestExpiration2())
                .maxRetryDelay(MAXRETRYDELAY).persistRetryDelay(PERSISTENT_RETRY_DELAY_MSEC)
                .persistRetryDelayUnit(TimeUnit.MILLISECONDS);
        producedTestPersist.set(0);
        processedTestPersist.set(0);
        queue.startQueue(config);
        for (int j = 0 ; j < (ROUNDS / 10); j++) {
            for (int i = 0 ; i < 10; i++) {
                producedTestPersist.incrementAndGet();
                queue.queueItem(new TestFileQueueItem(j*10 + i));
            }
            queue.stopQueue();
            queue.startQueue(config);
        }
        System.out.println("start/stops: "+ROUNDS);
        done(queue, producedTestPersist,processedTestPersist, null, ROUNDS);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    /* Test With Blockage */

    @Test
    public void testBlockage() throws Exception {
        String queueName = "testWithBlockage";
        Path db = setup("filequeue test with blockage", queueName, producedTestWithRetries, processedTestWithRetries);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestBlockConsumer(), executorService).maxQueueSize(MAXQUEUESIZE);
        queue.startQueue(config);
        producedTestWithBlockage.set(0);
        processedTestWithBlockage.set(0);
        block = true;
        for (int i = 0; i < BLOCKS; i++) {
            producedTestWithBlockage.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        block = false;
        try { Thread.sleep(4000); } catch (Exception e) {}
        for (int i = 0; i < ROUNDS - BLOCKS; i++) {
            producedTestWithBlockage.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        done(queue, producedTestWithBlockage, processedTestWithBlockage, null, ROUNDS);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    /* Implement Queue Item */

    private Path setup(String comment, String queueName, AtomicInteger produced, AtomicInteger processed) throws Exception {
        System.out.println(comment);
        produced.set(0);
        processed.set(0);
        Path db = Paths.get(File.separator + "tmp", queueName, queueName);
        try {
            if (Files.exists(db))
                MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        } catch (NotDirectoryException ignored) {
            Files.delete(db);
        }
        Files.createDirectories(db);
        return db;
    }


    /* Implement File Queue */

    private void done(FileQueue queue, AtomicInteger produced, AtomicInteger processed, Map<String,AtomicInteger> retries, int max) throws Exception {
        while (processed.get() < max) {
            Thread.sleep(1000);
        }
        System.out.println("processed: " + processed.get() + " produced: " + produced.get());
        Assert.assertEquals(produced.get(),processed.get());
        if (retries!=null) {
            int r = 0;
            for (AtomicInteger i : retries.values()) {
                Assert.assertEquals(RETRIES, i.get());
                r = r + i.get();
            }
            System.out.println("actual retries:" + r + " expected total retries: " + RETRIES * ROUNDS);
            Assert.assertEquals(RETRIES * ROUNDS, r);
        }
        Assert.assertEquals(0,queue.getQueueSize());
    }

    static class TestFileQueueItem extends FileQueueItem {

        Integer id;

        public TestFileQueueItem() { super(); }

        public TestFileQueueItem(Integer id) {
            super();
            this.id = id;
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }

        public Integer getId() {
            return id;
        }

    }


    /* Implement Throwable Queue Item */

    class TestThrowableConsumer implements Consumer<FileQueueItem> {

        public TestThrowableConsumer() {
        }

        @Override
        public Result consume(FileQueueItem item){
            processedTestWithThrowable.incrementAndGet();
            throw new RuntimeException("error");
        }
    }

    /* Implement Throwable Queue Item */

    class TestWithWaitConsumer implements Consumer<FileQueueItem> {

        public TestWithWaitConsumer() {
        }

        @Override
        public Result consume(FileQueueItem item){
            try {
                Thread.sleep(1000);
                processedTestWithWait.incrementAndGet();
                return Result.SUCCESS;
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                return Result.FAIL_NOQUEUE;
            }
        }
    }

    
    /* Implement Queue Item */

    class TestConsumer implements Consumer<FileQueueItem> {

        public TestConsumer() {
        }

        @Override
        public Result consume(FileQueueItem item){
            processedTestWithoutRetries.incrementAndGet();
            return Result.SUCCESS;
        }
    }


    static void incRetry(FileQueueItem item, Map<String,AtomicInteger> retries) {
        AtomicInteger itemTries;
        synchronized(retries) {
            itemTries = retries.get(item.toString());
            if (itemTries == null) itemTries = new AtomicInteger(0);
            itemTries.incrementAndGet();
            retries.put(item.toString(),itemTries);
        }

    }
    static Consumer.Result retry(FileQueueItem item, AtomicInteger processed, Map<String,AtomicInteger> retries) {
        try {
            incRetry(item,retries);
            TestFileQueueItem retryFileQueueItem = (TestFileQueueItem) item;
            if (retryFileQueueItem.getTryCount() == RETRIES) {
                processed.incrementAndGet();
                return Consumer.Result.SUCCESS;
            } else {
                return Consumer.Result.FAIL_REQUEUE;
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
            return Consumer.Result.FAIL_REQUEUE;
        }
    }

    static class TestRetryConsumer2 implements Consumer<FileQueueItem> {

        public TestRetryConsumer2() { }

        public Result consume(FileQueueItem item) {
            return retry(item, processedTestWithRetries, retryTestWithRetries);
        }
    }


    static class TestBlockConsumer implements Consumer<TestFileQueueItem> {

        public TestBlockConsumer() { }

        public Result consume(TestFileQueueItem item) {
            if (item.getId() < BLOCKS) {
                while(block) { try { Thread.sleep(10); } catch (Exception interrupted) { Thread.currentThread().interrupt(); } };
            }
            processedTestWithBlockage.incrementAndGet();
            return Result.SUCCESS;
        }
    }


    static class TestRetryConsumer3  implements Consumer<FileQueueItem> {

        public TestRetryConsumer3() { }

        public Result consume(FileQueueItem item) {
            return retry(item, processedTestWithRetryAndExponentialDelay, retryTestWithRetryAndExponentialDelay);
        }
    }

    static class TestExpireConsumer implements Consumer<FileQueueItem> {

        public TestExpireConsumer() { }

        public Result consume(FileQueueItem item){
            processedTestWithExpiry.incrementAndGet();
            incRetry(item,retryTestWithExpiry);
            return Result.FAIL_REQUEUE;
        }
    }


    static class TestExpireConsumer2 implements Consumer<FileQueueItem> {

        public TestExpireConsumer2() { }

        public Result consume(FileQueueItem item){
            incRetry(item,retryTestWithRetriesAndWait);
            if (item.getTryCount() == RETRIES) {
                processedTestWithRetriesAndWait.incrementAndGet();
                return Result.SUCCESS;
            } else
                return Result.FAIL_REQUEUE;
        }
    }



    static class TestExpiration implements Expiration<FileQueueItem> {

        @Override
        public void expire(FileQueueItem item) {
            expireTestWithExpiry.incrementAndGet();
        }
    }

    static class TestExpiration2 implements Expiration<FileQueueItem> {

        @Override
        public void expire(FileQueueItem item) {
            throw new RuntimeException("should not expire!!!");
        }
    }


    static class TestShutdownConsumer implements Consumer<FileQueueItem> {

        public TestShutdownConsumer() { }

        public Result consume(FileQueueItem item) throws InterruptedException {
            processedTestPersist.incrementAndGet();
            return Result.SUCCESS;
        }
    }

}
