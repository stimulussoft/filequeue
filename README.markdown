# Introduction
The FileQueue project offers a light weight, high performance, simple, reliable and persistent queue for Java applications. All Producers and consumers are run within a single Java runtime.
To provide persistence, FileQueue leverages the [MVStore](http://www.h2database.com/html/mvstore.html) database engine from H2. Queue items are regular Java POJOs, serialized into Json using [jackson](http://jackson.codehaus.org/).

To attain higher levels of performance, FileQueue will transfer queued items directly to consumers without hitting the database provided there are consumers available. If all consumers are busy, file queue will automatically persistent queued items to the database.

FileQueue also offers basic retry logic. It can be configured to redeliver items that could not be processed at a later stage.

# Usage

The steps for integration are as follows:

  1. Include maven POM or clone & compile the git repo

    <dependency>
        <groupId>com.stimulussoft</groupId>
        <artifactId>filequeue</artifactId>
        <version>1.0.4</version>
        <scope>test</scope>
    </dependency>

  2. Implement FileQueueItem or extend RetryQueueItem (for retry support)
  3. Extend FileQueue
    1. implement getFileQueueClass to return class created in step 1) above
    2. implement processFileQueueItem(FileQueueItem item) to perform actual processing work
  4. Call init(..) to initialize the queue
  5. Call startQueue() to start the queue
  6. Call stopQueue() to stop the queue processing

For API docs, refer to the file queue [JavaDoc](http://javadoc.io/doc/com.stimulussoft/filequeue/1.0.4).

Here's an example snippet of code showing the creation of the queue, a client sending pushing some messages and the consumption of the messages.

    RetryFileQueue  queue = new RetryFileQueue();
    queue.init(queueName, db, MAXQUEUESIZE);
    queue.setMaxTries(RETRIES);
    queue.setTryDelaySecs(RETRYDELAY);
    queue.startQueue();
    for (int i = 0; i < ROUNDS; i++) {
        queue.queueItem(new RetryFileQueueItem(i));
    }

    // when finished call stopQueue
    queue.stopQueue();

The FileQueue itself.

    import com.stimulussoft.filequeue.*;

    static class RetryFileQueue extends FileQueue {

        public RetryFileQueue() {

        }

        @Override
        public Class getFileQueueItemClass() {
            return RetryFileQueueItem.class;
        }

        @Override
        public ProcessResult processFileQueueItem(FileQueueItem item) throws InterruptedException {
            try {
                TestRetryFileQueueItem retryFileQueueItem = (TestRetryFileQueueItem) item;
                if (retryFileQueueItem.getTryCount() == RETRIES -1 ) {
                    processedTest2.incrementAndGet();
                    return ProcessResult.PROCESS_SUCCESS;
                }
                return ProcessResult.PROCESS_FAIL_REQUEUE;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return ProcessResult.PROCESS_FAIL_NOQUEUE;
            }
        }
    }

FileQueueItem PoJo. You can store anything in this object, provided it is compatible with Jackon serialization/deserialization.

    import com.stimulussoft.filequeue.*;

    static class TestRetryFileQueueItem extends RetryQueueItem {

      Integer id;


      public TestRetryFileQueueItem() {};

      private TestRetryFileQueueItem(Integer id) {
          this.id = id;
      }

      @Override
      public String toString() {
          return String.valueOf(id);
      }

      public Integer getId() { return id; }

    }



Refer to the FileQueueTest in the distribution for a working example.


# Credits
FileQueue is copyright Stimulus Software, implemented by Valentin Popov and Jamie Band.

Thanks for Martin Grotze for his original work on Persistent Queue. FileQueue now uses a completely different approach and
implementation.

Any contributions are welcome, so if you're missing the functionality you need, you're invited to submit a pull request or just [submit an issue](https://github.com/stimulussoft/filequeue/issues).

# License
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. You may obtain a copy of the License at ttp://www.apache.org/licenses/LICENSE-2.0.
