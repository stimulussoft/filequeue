# Introduction

The File Queue project offers a light weight, high performance, simple, reliable and persistent queue for Java applications. All producers and consumers run within a single Java runtime.
To provide persistence, File Queue leverages the [MVStore](http://www.h2database.com/html/mvstore.html) database engine from H2. Queue items are regular Java POJOs, serialized into Json using [jackson](http://jackson.codehaus.org/).

To attain higher levels of performance, File Queue will transfer queued items directly to consumers without hitting the database provided there are consumers available. If all consumers are busy, file queue will automatically persist queued items to the database.

File Queue also offers both fixed and exponential back-off retry.

# Why Reinvent the Wheel?

For our project, we needed a simple, light weight, high performance persistent queue written in Java. We tried them all, and could not find one that met our needs. 

# Usage

The steps for integration are as follows:

  1. Include maven POM or clone & compile the git repo

    <dependency>
        <groupId>com.stimulussoft</groupId>
        <artifactId>filequeue</artifactId>
        <version>1.0.29</version>
        <scope>test</scope>
    </dependency>

  2. Extend Consumer<FileQueueItem> and implement consume(FileQueueItem) to perform actual processing work.
  3. Create a new FileQueue object
  4. Call config() to construct an appropriate configuration
  5. Call startQueue() to start the queue
  6. Call stopQueue() to stop the queue processing

For API docs, refer to the File Queue [JavaDocs](http://javadoc.io/doc/com.stimulussoft/filequeue/1.0.29).

Here's an example snippet of code showing the creation of the queue, a client sending pushing some messages and the consumption of the messages. 

    FileQueue queue = FileQueue.fileQueue();
    FileQueue.Config config = FileQueue.config(queueName,queuePath,TestFileQueueItem.class, new TestConsumer())
                              .maxQueueSize(MAXQUEUESIZE)
                              .retryDelayAlgorithm(QueueProcessor.RetryDelayAlgorithm.EXPONENTIAL)
                              .retryDelay(RETRYDELAY).maxRetryDelay(MAXRETRYDELAY)
                              .maxRetries(0);
                              .persistRetryDelay(PERSISTENTRETRYDELAY);
    queue.startQueue(config);
    for (int i = 0; i < ROUNDS; i++)
        queue.queueItem(new TestFileQueueItem(i));
    // when finished call stopQueue
    queue.stopQueue();

In the above example, file queue retry policy is configured for exponential backoff. Set maxRetries to zero for infinite retries. 
The persistRetryDelay option specifies the delay between database scans. 

The FileQueue itself.

    import com.stimulussoft.filequeue.processor.*;

    static class TestConsumer implements Consumer<FileQueueItem> {

        public TestConsumer() { }

        @Override
        public Result consume(FileQueueItem item) throws InterruptedException {
            try {
                TestFileQueueItem retryFileQueueItem = (TestFileQueueItem) item;
                if (retryFileQueueItem.getTryCount() == RETRIES )
                    return Result.SUCCESS;
                return Result.FAIL_REQUEUE;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return Result.FAIL_NOQUEUE;
            }
        }
    }

FileQueueItem PoJo. You can store anything in this object, provided it is compatible with Jackon serialization/deserialization.

    import com.stimulussoft.filequeue.*;

    static class TestFileQueueItem extends FileQueueItem {

      Integer id;

      public TestFileQueueItem() { super(); };

      private TestFileQueueItem(Integer id) {
          this.id = id;
      }

      @Override
      public String toString() { return String.valueOf(id); }
      public Integer getId() { return id; }
      public void setId(Integer id) { this.id = id; }

    }



Refer to the FileQueueTest in the distribution for working examples.


# Credits
FileQueue is copyright Stimulus Software, implemented by Valentin Popov and Jamie Band.

Thanks for Martin Grotze for his original work on Persistent Queue. FileQueue now uses a completely different approach and
implementation.

Any contributions are welcome, so if you're missing the functionality you need, you're invited to submit a pull request or just [submit an issue](https://github.com/stimulussoft/filequeue/issues).

# License
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. You may obtain a copy of the License at ttp://www.apache.org/licenses/LICENSE-2.0.
