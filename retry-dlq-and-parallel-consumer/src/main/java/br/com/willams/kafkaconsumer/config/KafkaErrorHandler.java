package br.com.willams.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.List;
import java.util.Random;

@Configuration
@Slf4j
public class KafkaErrorHandler {
    private static Random randomPartition = new Random();

    private final KafkaTemplate kafkaTemplate;

    private final String retryTopic;

    private final String deadLetterTopic;

    public KafkaErrorHandler(KafkaTemplate kafkaTemplate,
                             @Value("${app.topics.retry}") String retryTopic,
                             @Value("${app.topics.dlq}") String deadLetterTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.retryTopic = retryTopic;
        this.deadLetterTopic = deadLetterTopic;
    }

    @Bean
    public DefaultErrorHandler errorHandler() {

        var exceptionToIgnorelist = List.of(
                IllegalArgumentException.class
        );

        var exceptionsToRetryList = List.of(
                RecoverableDataAccessException.class
        );

        var expBackOff = new ExponentialBackOffWithMaxRetries(2);

        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);

        var errorHandler = new DefaultErrorHandler(publishingRecoverer(), expBackOff);

        exceptionToIgnorelist.forEach(errorHandler::addNotRetryableExceptions);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt)
        );

        return errorHandler;
    }

    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishing Recoverer : {} ", e.getMessage(), e);

                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        var patition = r.partition() < 3 ? r.partition() : randomPartition.nextInt() % 3;
                        var topic = r.topic() + ".retry-" + patition;

                        return new TopicPartition(topic, patition);
                    } else {
                        r.headers().add("Exception", e.getMessage().getBytes());
                        return new TopicPartition(r.topic() + ".dlq", -1);
                    }
                });

        return recoverer;

    }

}
