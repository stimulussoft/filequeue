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

package com.stimulussoft.util;

import java.lang.ref.Reference;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadUtil {

    public static final int DEFAULT_CORE_POOL_SIZE = 1;

    public static ThreadFactory getFlexibleThreadFactory(String name) {
        return new FlexibleThreadFactory(name, false);
    }

    public static ThreadFactory getFlexibleThreadFactory(String name, boolean daemon) {
        return new FlexibleThreadFactory(name, daemon);
    }

    public static ThreadFactory getGroupThreadFactory(ThreadGroup group, String threadName, boolean daemon) {
        return new ThreadGroupThreadFactory(group, threadName, daemon);
    }

    public static void shutdownAndAwaitTermination(ExecutorService pool, long timeout, TimeUnit timeUnit) throws InterruptedException {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(timeout, timeUnit)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                pool.awaitTermination(timeout, timeUnit);
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            throw ie;
        }
    }

    public static void cleanThreadLocals2() {
        try {
            // Get a reference to the thread locals table of the current thread
            Thread thread = Thread.currentThread();
            Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
            threadLocalsField.setAccessible(true);
            Object threadLocalTable = threadLocalsField.get(thread);

            // Get a reference to the array holding the thread local variables inside the
            // ThreadLocalMap of the current thread
            Class threadLocalMapClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
            Field tableField = threadLocalMapClass.getDeclaredField("table");
            tableField.setAccessible(true);
            Object table = tableField.get(threadLocalTable);

            // The key to the ThreadLocalMap is a WeakReference object. The referent field of this object
            // is a reference to the actual ThreadLocal variable
            Field referentField = Reference.class.getDeclaredField("referent");
            referentField.setAccessible(true);

            for (int i = 0; i < Array.getLength(table); i++) {
                // Each entry in the table array of ThreadLocalMap is an Entry object
                // representing the thread local reference and its value
                Object entry = Array.get(table, i);
                if (entry != null) {
                    // Get a reference to the thread local object and remove it from the table
                    ThreadLocal threadLocal = (ThreadLocal) referentField.get(entry);
                    threadLocal.remove();
                }
            }
        } catch (Exception e) {
            // We will tolerate an exception here and just log it
            throw new IllegalStateException(e);
        }
    }

    public static void checkInterrupt() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            Thread.currentThread().interrupt();
            throw new InterruptedException("interrupted");
        }
    }

    public static ExecutorService newFlexiThreadPool(int corePoolSize, int maximumPoolSize, ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }

    public static ExecutorService newFlexiThreadPool(int maximumPoolSize, ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(DEFAULT_CORE_POOL_SIZE, maximumPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }

    private static class ThreadGroupThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        private final boolean daemon;

        ThreadGroupThreadFactory(ThreadGroup group, String threadName, boolean daemon) {
            this.group = (group != null) ? group : Thread.currentThread().getThreadGroup();
            this.daemon = daemon;
            namePrefix = threadName + "-pool-" + poolNumber.getAndIncrement() + "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            t.setDaemon(daemon);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    private static class FlexibleThreadFactory implements ThreadFactory {

        private final AtomicInteger threadNumber = new AtomicInteger(1);
        protected String name;
        protected boolean daemon;

        public FlexibleThreadFactory(String name, boolean daemon) {
            this.name = name;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(daemon);
            t.setName(name + "-" + threadNumber.incrementAndGet());
            return t;
        }

    }


}
