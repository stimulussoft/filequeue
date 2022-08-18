package com.stimulussoft.filequeue.processor;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Valentin Popov valentin@archiva.ru on 16.08.2022.
 */
public final class QueueProcessorBuilder<T> {

    protected Path queuePath;
    protected String queueName;
    protected Type type;
    protected int maxTries = 0;
    protected int retryDelay = 1;
    protected int persistRetryDelay = 0;
    protected int maxRetryDelay = 1;
    protected TimeUnit retryDelayUnit = TimeUnit.SECONDS;
    protected TimeUnit persistRetryDelayUnit = TimeUnit.SECONDS;
    protected Consumer<T> consumer;
    protected Expiration<T> expiration;
    protected ExecutorService executorService;
    protected QueueProcessor.RetryDelayAlgorithm retryDelayAlgorithm = QueueProcessor.RetryDelayAlgorithm.FIXED;
    protected ObjectMapper objectMapper = null;
    protected int maxQueueSize = Integer.MAX_VALUE;

    public static <T1> QueueProcessorBuilder<T1> builder(String queueName, Path queuePath, Class<T1> type,
                                                     Consumer<T1> consumer, ExecutorService executor) throws IllegalArgumentException {
        return new QueueProcessorBuilder<>(queueName, queuePath, type, consumer, executor);
    }

    public static QueueProcessorBuilder<Object> builder() {
        return new QueueProcessorBuilder<>();
    }

    private QueueProcessorBuilder() {

    }

    private QueueProcessorBuilder(String queueName, Path queuePath, Class type, Consumer<T> consumer, ExecutorService executorService) throws IllegalArgumentException {
        if (queueName == null) throw new IllegalArgumentException("queue name must be specified");
        if (queuePath == null) throw new IllegalArgumentException("queue path must be specified");
        if (type == null) throw new IllegalArgumentException("item type must be specified");
        if (consumer == null) throw new IllegalArgumentException("consumer must be specified");
        this.queueName = queueName;
        this.queuePath = queuePath;
        this.type = type;
        this.consumer = consumer;
        this.executorService = executorService;
    }

    /**
     * Queue path
     *
     * @param queuePath path to queue database
     * @return builder
     */
    public QueueProcessorBuilder queuePath(Path queuePath) {
        this.queuePath = queuePath;
        return this;
    }

    public Path getQueuePath() {
        return queuePath;
    }

    /**
     * Queue name
     *
     * @param queueName friendly name for the queue
     * @return builder
     */
    public QueueProcessorBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public String getQueueName() {
        return queueName;
    }

    /**
     * Type of queue item
     *
     * @param type filequeueitem type
     * @return builder
     */
    public QueueProcessorBuilder type(Type type) {
        this.type = type;
        return this;
    }

    public Type getType() {
        return type;
    }

    /**
     * Maximum size of the queue before blocking
     *
     * @param maxQueueSize maximum queue size
     * @return builder
     */
    public QueueProcessorBuilder maxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
        return this;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    /**
     * Maximum number of tries. Set to zero for infinite.
     *
     * @param maxTries maximum number of retries
     * @return builder
     */
    public QueueProcessorBuilder maxTries(int maxTries) {
        this.maxTries = maxTries;
        return this;
    }

    public int getMaxTries() {
        return maxTries;
    }

    /**
     * Set fixed delay in retryDelayUnit between retries
     *
     * @param retryDelay delay between retries
     * @return builder
     */
    public QueueProcessorBuilder retryDelay(int retryDelay) {
        this.retryDelay = retryDelay;
        return this;
    }

    public int getRetryDelay() {
        return retryDelay;
    }

    /**
     * Set maximum delay in retryDelayUnit between retries assuming exponential backoff enabled
     *
     * @param maxRetryDelay maximum delay between retries
     * @return builder
     */
    public QueueProcessorBuilder maxRetryDelay(int maxRetryDelay) {
        this.maxRetryDelay = maxRetryDelay;
        return this;
    }

    public int getMaxRetryDelay() {
        return maxRetryDelay;
    }

    /**
     * Set delay between retries in persistRetryDelayUnit when processing items from queue database (on disk). Items are only put on disk
     * when the in-memory-processing-queue is full
     *
     * @param persistRetryDelay maximum delay between retries for items on disk
     * @return builder
     */
    public QueueProcessorBuilder persistRetryDelay(int persistRetryDelay) {
        this.persistRetryDelay = persistRetryDelay;
        return this;
    }

    public int getPersistRetryDelay() {
        return persistRetryDelay;
    }

    /**
     * Set persistent retry delay time unit. Default is seconds.
     *
     * @param persistRetryDelayUnit persistent retry delay time unit
     * @return builder
     */
    public QueueProcessorBuilder persistRetryDelayUnit(TimeUnit persistRetryDelayUnit) {
        this.persistRetryDelayUnit = persistRetryDelayUnit;
        return this;
    }

    public TimeUnit getPersistRetryDelayUnit() {
        return persistRetryDelayUnit;
    }

    /**
     * Set retry delay time unit. Default is seconds.
     *
     * @param retryDelayUnit retry delay time unit
     * @return builder
     */
    public QueueProcessorBuilder retryDelayUnit(TimeUnit retryDelayUnit) {
        this.retryDelayUnit = retryDelayUnit;
        return this;
    }

    public TimeUnit getRetryDelayUnit() {
        return retryDelayUnit;
    }

    /**
     * Set retry delay algorithm (FIXED or EXPONENTIAL)
     *
     * @param retryDelayAlgorithm set to either fixed or exponential backoff
     * @return builder
     */
    public QueueProcessorBuilder retryDelayAlgorithm(QueueProcessor.RetryDelayAlgorithm retryDelayAlgorithm) {
        this.retryDelayAlgorithm = retryDelayAlgorithm;
        return this;
    }

    public QueueProcessor.RetryDelayAlgorithm getRetryDelayAlgorithm() {
        return retryDelayAlgorithm;
    }

    /**
     * Set retry delay consumer
     *
     * @param consumer retry delay consumer
     * @return builder
     */
    public QueueProcessorBuilder<T> consumer(Consumer<T> consumer) {
        this.consumer = consumer;
        return this;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    /**
     * Executor service
     *
     * @param executorService executor Service
     * @return builder
     */
    public QueueProcessorBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public ExecutorService executorService() {
        return executorService;
    }

    /**
     * Set retry delay expiration
     *
     * @param expiration retry delay expiration
     * @return builder
     */
    public QueueProcessorBuilder<T> expiration(Expiration<T> expiration) {
        this.expiration = expiration;
        return this;
    }

    public Expiration<T> getExpiration() {
        return expiration;
    }

    public QueueProcessorBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }

    public <T1 extends T> QueueProcessor build() throws IOException, IllegalStateException, IllegalArgumentException, InterruptedException {
        return new QueueProcessor<>(this);
    }
}
