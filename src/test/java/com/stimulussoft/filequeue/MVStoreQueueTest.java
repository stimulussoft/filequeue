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
 *
 * @author Valentin Popov
 */


package com.stimulussoft.filequeue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static junit.framework.Assert.*;


/* MVStoreQueueTest
 * Tests internal MVStoreQueueTest class. Rather extend FileQueue and RetryFileQueueItem.
 * @author Valentin Popov
 */


public class MVStoreQueueTest {

    MVStoreQueue queue;
    ConcurrentLinkedQueue<byte[]> producerQueue = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<byte[]> consumerQueue = new ConcurrentLinkedQueue<>();
    int toGenerate = 100000;
    CountDownLatch toPush = new CountDownLatch(toGenerate);
    CountDownLatch toPoll = new CountDownLatch(toGenerate);


    @Before
    public void prepare() throws IOException {
        queue = new MVStoreQueue("test");
    }

    @After
    public void stop() {
        queue.clear();
        queue.close();
    }

    @Test
    public void consistency() throws IOException {
        List<byte[]> added = new ArrayList<>(toGenerate);

        for (int i = 0; i < toGenerate; i++) {
            byte[] toAdd = Integer.toHexString(i).getBytes();
            queue.push(toAdd);
            added.add(toAdd);
        }

        assertEquals(toGenerate, queue.size());

        List<byte[]> consumed = new ArrayList<>(toGenerate);
        byte[] element;

        while ((element = queue.poll()) != null) {
            consumed.add(element);
        }

        assertEquals(toGenerate, consumed.size());
        consumed.removeAll(added);
        assertTrue(consumed.size() == 0);
    }

    @Test
    public void concurentConsistency() throws InterruptedException {
        int threadsNumber = 10;

        ExecutorService producerExecutor = Executors.newFixedThreadPool(threadsNumber);
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(threadsNumber);

        for (int i = 0; i < threadsNumber; i++) {
            consumerExecutor.submit(new Consumer(queue));
            producerExecutor.submit(new Producer(queue));
        }

        if (!toPush.await(1, TimeUnit.MINUTES)) fail("Timeout produce");
        producerExecutor.shutdown();

        if (!toPoll.await(1, TimeUnit.MINUTES)) fail("Timeout consume");
        consumerExecutor.shutdown();

        assertEquals(producerQueue.size(), consumerQueue.size());

        producerQueue.removeAll(consumerQueue);
        assertTrue(producerQueue.size() == 0);
    }

    class Producer implements Runnable {

        private MVStoreQueue queue;

        public Producer(MVStoreQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (toPush.getCount() != 0) {
                toPush.countDown();
                long current = toPush.getCount();
                byte[] toAdd = Long.toHexString(current).getBytes();
                queue.push(toAdd);
                producerQueue.add(toAdd);
            }
        }
    }

    class Consumer implements Runnable {
        private MVStoreQueue queue;

        public Consumer(MVStoreQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (toPoll.getCount() != 0) {
                byte[] val = queue.poll();
                if (val != null) {
                    toPoll.countDown();
                    consumerQueue.add(val);
                }
            }
        }
    }

}