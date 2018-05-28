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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/* DB Processing Queue
 * Tests internal QueueProcessor class. Rather extend FileQueue and RetryFileQueueItem.
 */

public class QueueProcessorTest {


    static AtomicInteger processed = new AtomicInteger(0);
    static AtomicInteger produced = new AtomicInteger(0);

    @Test
    public void oneQueue() throws IOException {

        Path dir = Files.createTempDirectory("filequeue");

        final Phaser phaser = new Phaser();
        TestQueue testQueue = new TestQueue(dir, "test_queue", Integer.class, 5, 1, TimeUnit.SECONDS,
                new AlwaysTrueConsumer(phaser));

        int threads = 128;
        int toProcess = 100;

        ExecutorService executorService = Executors.newFixedThreadPool(threads,
                new ThreadFactoryBuilder().setDaemon(false).setNameFormat("producer-%d").build());


        final Phaser starter = new Phaser(1);

        for (int i = 0; i < threads; i++) {
            starter.register();
            executorService.submit(() -> {
                starter.arriveAndAwaitAdvance();
                for (int i1 = 0; i1 < toProcess; i1++) {
                    try {
                        phaser.register();
                        testQueue.submit(i1);
                        produced.incrementAndGet();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        starter.arriveAndDeregister();

        phaser.register();
        while (!phaser.isTerminated()) {
            phaser.arriveAndAwaitAdvance();
            if (processed.get() == threads * toProcess) {
                phaser.forceTermination();
            }
        }

        executorService.shutdown();

        Assert.assertTrue(processed.get() == produced.get());
    }

    private static class AlwaysTrueConsumer implements Consumer<Integer> {

        private final Phaser phaser;

        public AlwaysTrueConsumer(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public boolean consume(Integer item) throws InterruptedException {
            Thread.sleep(100);
            phaser.arriveAndDeregister();
            processed.incrementAndGet();
            return true;
        }
    }

    private static class TestQueue extends QueueProcessor<Integer> {

        public TestQueue(Path queueROOT, String queueName, Class<Integer> type, int maxTries, int retryDelay, TimeUnit retryDelayTimeUnit, Consumer<Integer> consumer) throws IOException {
            super(queueROOT, queueName, type, maxTries, retryDelay, retryDelayTimeUnit, consumer,1, TimeUnit.SECONDS);
        }
    }

}