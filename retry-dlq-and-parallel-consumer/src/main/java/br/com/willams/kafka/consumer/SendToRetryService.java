package br.com.willams.kafka.consumer;

public interface SendToRetryService<T> {
    void processRetry(T data, ConsumerProcessor consumerProcessor);
}