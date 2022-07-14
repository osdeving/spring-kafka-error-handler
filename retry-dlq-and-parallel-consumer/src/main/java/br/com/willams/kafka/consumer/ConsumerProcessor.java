package br.com.willams.kafka.consumer;

public interface ConsumerProcessor<T> {
    boolean process(T event);
}