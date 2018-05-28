package com.stimulussoft.filequeue;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

/* File Queue Test
 * Demonstrates how to use the filequeue
 * @author Valentin Popov
 */

public class FileQueueTest {

    private static final int ROUNDS = 200;
    private static final int RETRIES = 10;
    private static final int RETRYDELAY = 0;
    private static final int MAXQUEUESIZE = 100;


    private static AtomicInteger processedTest1 = new AtomicInteger(0);
    private static AtomicInteger producedTest1 = new AtomicInteger(0);
    private static AtomicInteger processedTest2 = new AtomicInteger(0);
    private static AtomicInteger producedTest2 = new AtomicInteger(0);

     /* Test Without Retries */

    @Test
    public void test1() throws Exception {
        String queueName = "test1";
        Path db = setup("filequeue test without retries", queueName, producedTest1, processedTest1);
        TestFileQueue queue = new TestFileQueue();
        queue.init(queueName, db, MAXQUEUESIZE);
        queue.startQueue();
        for (int i = 0; i < ROUNDS; i++) {
            producedTest1.incrementAndGet();
            queue.queueItem(new TestFileQueueItem(i));
        }
        done(producedTest1, processedTest1);
        queue.stopQueue();
        MoreFiles.deleteDirectoryContents(db, RecursiveDeleteOption.ALLOW_INSECURE);

    }

     /* Test With Retries */

    @Test
    public void test2() throws Exception {
        String queueName = "test2";
        Path db = setup("filequeue test with retries", queueName, producedTest2, processedTest2);
        TestRetryFileQueue queue = new TestRetryFileQueue();
        queue.init(queueName, db, MAXQUEUESIZE);
        queue.setMaxTries(RETRIES);
        queue.setTryDelaySecs(RETRYDELAY);
        queue.startQueue();
        for (int i = 0; i < ROUNDS; i++) {
            producedTest2.incrementAndGet();
            queue.queueItem(new TestRetryFileQueueItem(i));
        }
        done(producedTest2, processedTest2);
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

    private void done(AtomicInteger produced, AtomicInteger processed) throws Exception {
        while (processed.get() < ROUNDS) {
            Thread.sleep(1000);
        }
        System.out.println("processed: " + processed.get() + " produced: " + produced.get());

        Assert.assertEquals(processed.get(), produced.get());
    }

    static class TestFileQueueItem extends FileQueueItem {

        Integer id;
        Boolean requeue;

        public TestFileQueueItem(Integer id) {
            this.id = id;
        }

        public TestFileQueueItem() {
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }

        public Integer getId() {
            return id;
        }

//        @Override
//        public Boolean call() throws Exception {
//            return true;
//        }
    }

    static class TestRetryFileQueue extends FileQueue {

        public TestRetryFileQueue() {

        }

        @Override
        public Class getFileQueueItemClass() {
            return TestRetryFileQueueItem.class;
        }

        @Override
        public ProcessResult processFileQueueItem(FileQueueItem item)  {
            try {
                TestRetryFileQueueItem retryFileQueueItem = (TestRetryFileQueueItem) item;
                //logger.debug("found item "+ retryFileQueueItem.getId() +" try count "+retryFileQueueItem.getTryCount());
                if (retryFileQueueItem.getTryCount() == RETRIES - 1) {
                    processedTest2.incrementAndGet();
                    return ProcessResult.PROCESS_SUCCESS;
                }
                return ProcessResult.PROCESS_FAIL_REQUEUE;
            } catch (Exception e) {
                e.printStackTrace();
                return ProcessResult.PROCESS_FAIL_NOQUEUE;
            }
        }

        @Override
        public void expiredItem(FileQueueItem item) {
            Assert.fail("there should be no expired items");
            throw new RuntimeException();
        }
    }



     /* Implement File Queue */

    static class TestRetryFileQueueItem extends FileQueueItem {

        Integer id;


        public TestRetryFileQueueItem() {
        }

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

//        @Override
//        public Boolean call() throws Exception {
//            return true;
//        }
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


}
