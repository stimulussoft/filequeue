package com.stimulussoft.filequeue;

import com.google.common.collect.Maps;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.stimulussoft.filequeue.processor.Consumer;
import com.stimulussoft.filequeue.processor.Expiration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/* File Queue Test
 * Demonstrates how to use the filequeue
 * @author Valentin Popov
 */

public class FileQueueTest {

    private static final int ROUNDS = 20000;
    private static final int RETRIES = 100;
    private static final int MAXRETRYDELAY = 64;
    private static final int RETRYDELAY = 60;
    private static final int MAXQUEUESIZE = 100;
    private static final TimeUnit RetryDelayTimeUnit = TimeUnit.MILLISECONDS;

    private static AtomicInteger processedTest1 = new AtomicInteger(0);
    private static AtomicInteger producedTest1 = new AtomicInteger(0);
    private static AtomicInteger processedTest2 = new AtomicInteger(0);
    private static AtomicInteger producedTest2 = new AtomicInteger(0);
    private static Map<String,AtomicInteger> retryTest2 = Maps.newConcurrentMap();
    private static AtomicInteger processedTest3 = new AtomicInteger(0);
    private static AtomicInteger processedTest4 = new AtomicInteger(0);
    private static AtomicInteger producedTest3 = new AtomicInteger(0);
    private static AtomicInteger producedTest4 = new AtomicInteger(0);
    private static Map<String,AtomicInteger> retryTest3 = Maps.newConcurrentMap();
    private static Map<String,AtomicInteger> retryTest4 = Maps.newConcurrentMap();
    private static AtomicInteger expireTest4 =  new AtomicInteger(0);
    /* Test Without Retries */

    @Test
    public void testWithoutRetries() throws Exception {
        String queueName = "test1";
        Path db = setup("filequeue test without retries", queueName, producedTest1, processedTest1);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestFileQueueItem.class,new TestConsumer()).maxQueueSize(MAXQUEUESIZE).persistRetryDelay(1);
        queue.startQueue(config);
        Assert.assertEquals(queue.getConfig().getQueueName(), queueName);
        Assert.assertEquals(queue.getConfig().getQueuePath(), db);
        Assert.assertEquals(queue.getConfig().getMaxQueueSize(), MAXQUEUESIZE);
        producedTest1.set(0);
        processedTest1.set(0);
        for (int i = 0; i < ROUNDS; i++) {
            producedTest1.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        done(producedTest1, processedTest1, null);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);

    }

     /* Test With Retries */

    @Test
    public void testWithRetries() throws Exception {
        String queueName = "test2";
        Path db = setup("filequeue test with retries", queueName, producedTest2, processedTest2);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestRetryFileQueueItem.class,new TestRetryConsumer2()).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                                  .retryDelay(RETRYDELAY).retryDelayUnit(RetryDelayTimeUnit).persistRetryDelay(1);
        queue.startQueue(config);
        Assert.assertEquals(queue.getConfig().getMaxTries(), RETRIES);
        Assert.assertEquals(queue.getConfig().getRetryDelay(), RETRYDELAY);
        Assert.assertEquals(queue.getConfig().getRetryDelayUnit(), RetryDelayTimeUnit);
        producedTest2.set(0);
        processedTest2.set(0);
        for (int i = 0; i < ROUNDS; i++) {
            producedTest2.incrementAndGet();
            queue.queueItem(new TestRetryFileQueueItem(i));
        }
        done(producedTest2, processedTest2, retryTest2);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void testWithRetryAndExponentialDelay() throws Exception {
        String queueName = "test3";
        Path db = setup("filequeue test with retries and exponential delay", queueName, producedTest3, processedTest3);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestRetryFileQueueItem.class,new TestRetryConsumer3()).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                .retryDelay(RETRYDELAY).retryDelayUnit(RetryDelayTimeUnit)
                .retryDelayAlgorithm(FileQueue.RetryDelayAlgorithm.EXPONENTIAL).retryDelay(RETRYDELAY).maxRetryDelay(MAXRETRYDELAY);
        queue.startQueue(config);
        producedTest3.set(0);
        processedTest3.set(0);
        for (int i = 0; i < ROUNDS; i++) {
            producedTest3.incrementAndGet();
            queue.queueItem(new TestRetryFileQueueItem(i));
        }
        done(producedTest3, processedTest3, retryTest3);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void testWithExpiry() throws Exception {
        String queueName = "test4";
        Path db = setup("filequeue test with expiry", queueName, producedTest4, processedTest4);
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
        FileQueue queue = FileQueue.fileQueue();
        FileQueue.Config config = FileQueue.config(queueName,db,TestRetryFileQueueItem.class,new TestExpireConsumer()).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                .retryDelay(RETRYDELAY).retryDelayUnit(RetryDelayTimeUnit).expiration(new TestExpiration())
                .retryDelayAlgorithm(FileQueue.RetryDelayAlgorithm.EXPONENTIAL).retryDelay(RETRYDELAY)
                .maxRetryDelay(MAXRETRYDELAY);
        queue.startQueue(config);
        producedTest4.set(0);
        processedTest4.set(0);
        for (int i = 0; i < ROUNDS; i++) {
            producedTest4.incrementAndGet();
            queue.queueItem(new TestRetryFileQueueItem(i));
        }
        done(producedTest4,expireTest4, retryTest4);
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

    private void done(AtomicInteger produced, AtomicInteger processed, Map<String,AtomicInteger> retries) throws Exception {
        while (processed.get() < ROUNDS) {
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


    /* Implement Queue Item */

    class TestConsumer implements Consumer<FileQueueItem> {

        public TestConsumer() {
        }

        @Override
        public Result consume(FileQueueItem item){
            processedTest1.incrementAndGet();
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
            TestRetryFileQueueItem retryFileQueueItem = (TestRetryFileQueueItem) item;
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
            return retry(item, processedTest2, retryTest2);
        }
    }

    static class TestRetryConsumer3  implements Consumer<FileQueueItem> {

        public TestRetryConsumer3() { }

        public Result consume(FileQueueItem item) {
            return retry(item, processedTest3, retryTest3);
        }
    }

    static class TestExpireConsumer implements Consumer<FileQueueItem> {

        public TestExpireConsumer() { }

        public Result consume(FileQueueItem item){
            processedTest4.incrementAndGet();
            incRetry(item,retryTest4);
            return Result.FAIL_REQUEUE;
        }
    }

    static class TestExpiration implements Expiration<FileQueueItem> {

        @Override
        public void expire(FileQueueItem item) {
            expireTest4.incrementAndGet();
        }
    }


    /* Implement File Queue */

    static class TestRetryFileQueueItem extends FileQueueItem {

        Integer id;

        public TestRetryFileQueueItem() { super(); }

        private TestRetryFileQueueItem(Integer id) {
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


}
