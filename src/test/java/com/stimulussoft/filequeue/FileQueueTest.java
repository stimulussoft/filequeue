package com.stimulussoft.filequeue;

import com.google.common.collect.Maps;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
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

    private static final int ROUNDS = 2000;
    private static final int RETRIES = 100;
    private static final int MAXRETRYDELAY = 64;
    private static final int RETRYDELAY = 1;
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
    private static Map<String,AtomicInteger> expireTest4 = Maps.newConcurrentMap();
     /* Test Without Retries */

    @Test
    public void test1() throws Exception {
        String queueName = "test1";
        Path db = setup("filequeue test without retries", queueName, producedTest1, processedTest1);
        TestFileQueue queue = new TestFileQueue();
        FileQueue.Config config = FileQueue.config().queueName(queueName).queuePath(db).type(TestFileQueueItem.class).maxQueueSize(MAXQUEUESIZE);
        queue.startQueue(config);

        Assert.assertEquals(queue.getConfig().getQueueName(), queueName);
        Assert.assertEquals(queue.getConfig().getQueuePath(), db);
        Assert.assertEquals(queue.getConfig().getMaxQueueSize(), MAXQUEUESIZE);

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
    public void test2() throws Exception {
        String queueName = "test2";
        Path db = setup("filequeue test with retries", queueName, producedTest2, processedTest2);
        TestRetryFileQueue2 queue = new TestRetryFileQueue2();
        FileQueue.Config config = FileQueue.config().queueName(queueName).queuePath(db).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                                  .retryDelay(RETRYDELAY).retryDelayTimeUnit(RetryDelayTimeUnit).type(TestRetryFileQueueItem.class);
        queue.startQueue(config);

        Assert.assertEquals(queue.getConfig().getMaxTries(), RETRIES);
        Assert.assertEquals(queue.getConfig().getRetryDelay(), RETRYDELAY);
        Assert.assertEquals(queue.getConfig().getRetryDelayTimeUnit(), RetryDelayTimeUnit);

        for (int i = 0; i < ROUNDS; i++) {
            producedTest2.incrementAndGet();
            queue.queueItem(new TestRetryFileQueueItem(i));
        }
        done(producedTest2, processedTest2, retryTest2);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void test3() throws Exception {
        String queueName = "test3";
        Path db = setup("filequeue test with retries and exponential delay", queueName, producedTest3, processedTest3);
        TestRetryFileQueue3 queue = new TestRetryFileQueue3();
        FileQueue.Config config = FileQueue.config().queueName(queueName).queuePath(db).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                .retryDelay(RETRYDELAY).retryDelayTimeUnit(RetryDelayTimeUnit)
                .retryDelayAlgorithm(FileQueue.RetryDelayAlgorithm.EXPONENTIAL).retryDelay(RETRYDELAY).maxRetryDelay(MAXRETRYDELAY).type(TestRetryFileQueueItem.class);
        queue.startQueue(config);
        for (int i = 0; i < ROUNDS; i++) {
            producedTest3.incrementAndGet();
            queue.queueItem(new TestRetryFileQueueItem(i));
        }
        done(producedTest3, processedTest3, retryTest3);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void test4() throws Exception {
        String queueName = "test4";
        Path db = setup("filequeue test with retries and expiry", queueName, producedTest4, processedTest4);
        TestExpireFileQueue queue = new TestExpireFileQueue();
        FileQueue.Config config = FileQueue.config().queueName(queueName).queuePath(db).maxQueueSize(MAXQUEUESIZE).maxTries(RETRIES)
                .retryDelay(RETRYDELAY).retryDelayTimeUnit(RetryDelayTimeUnit)
                .retryDelayAlgorithm(FileQueue.RetryDelayAlgorithm.EXPONENTIAL).retryDelay(RETRYDELAY)
                .maxRetryDelay(MAXRETRYDELAY).type(TestRetryFileQueueItem.class);
        queue.startQueue(config);
        for (int i = 0; i < ROUNDS; i++) {
            producedTest4.incrementAndGet();
            queue.queueItem(new TestRetryFileQueueItem(i));
        }
        done(producedTest4, processedTest4, expireTest4);
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
        Assert.assertEquals(processed.get(), produced.get());
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

        public TestFileQueueItem(Integer id) {
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

    class TestFileQueue extends FileQueue {

        public TestFileQueue() {
        }

        @Override
        public Class getFileQueueItemClass() {
            return TestFileQueueItem.class;
        }

        @Override
        public ProcessResult processFileQueueItem(FileQueueItem item){
            processedTest1.incrementAndGet();
            return ProcessResult.PROCESS_SUCCESS;
        }

        @Override
        public void expiredItem(FileQueueItem item) {
            Assert.fail("there should be no expired items");
            throw new RuntimeException();
        }
    }


    static FileQueue.ProcessResult retry(FileQueueItem item, AtomicInteger processed, Map<String,AtomicInteger> retries) {
        try {
            AtomicInteger itemTries;
            synchronized(retries) {
                itemTries = retries.get(item.toString());
                if (itemTries == null) itemTries = new AtomicInteger(0);
                itemTries.incrementAndGet();
                retries.put(item.toString(),itemTries);
            }

            TestRetryFileQueueItem retryFileQueueItem = (TestRetryFileQueueItem) item;
            if (retryFileQueueItem.getTryCount() == RETRIES) {
                processed.incrementAndGet();
                return FileQueue.ProcessResult.PROCESS_SUCCESS;
            } else {
                return FileQueue.ProcessResult.PROCESS_FAIL_REQUEUE;
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
            return FileQueue.ProcessResult.PROCESS_FAIL_REQUEUE;
        }
    }

    static class TestRetryFileQueue2 extends FileQueue {

        public TestRetryFileQueue2() {

        }

        @Override
        public Class getFileQueueItemClass() {
            return TestRetryFileQueueItem.class;
        }

        @Override
        public ProcessResult processFileQueueItem(FileQueueItem item)  {
            return retry(item, processedTest2, retryTest2);
        }

        @Override
        public void expiredItem(FileQueueItem item) {
            Assert.fail("there should be no expired items");
            throw new RuntimeException();
        }
    }


    static class TestRetryFileQueue3 extends FileQueue {

        public TestRetryFileQueue3() {

        }

        @Override
        public Class getFileQueueItemClass() {
            return TestRetryFileQueueItem.class;
        }

        @Override
        public ProcessResult processFileQueueItem(FileQueueItem item)  {
            return retry(item, processedTest3, retryTest3);
        }

        @Override
        public void expiredItem(FileQueueItem item) {
            Assert.fail("there should be no expired items");
            throw new RuntimeException();
        }
    }


    static class TestExpireFileQueue extends FileQueue {

        public TestExpireFileQueue() { }

        @Override
        public Class getFileQueueItemClass() {
            return TestRetryFileQueueItem.class;
        }

        @Override
        public ProcessResult processFileQueueItem(FileQueueItem item)  {
             return FileQueue.ProcessResult.PROCESS_FAIL_REQUEUE;
        }

        @Override
        public void expiredItem(FileQueueItem item) {
            processedTest4.incrementAndGet();
            AtomicInteger itemTries;
            synchronized(expireTest4) {
                itemTries = expireTest4.get(item.toString());
                if (itemTries == null) itemTries = new AtomicInteger(item.getTryCount()-1);
                expireTest4.put(item.toString(),itemTries);
            }
        }
    }




    /* Implement File Queue */

    static class TestRetryFileQueueItem extends FileQueueItem {

        Integer id;

        public TestRetryFileQueueItem() { }

        private TestRetryFileQueueItem(Integer id) {
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
