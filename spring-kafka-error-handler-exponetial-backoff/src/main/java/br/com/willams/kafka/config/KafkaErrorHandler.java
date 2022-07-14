package br.com.willams.kafka.config;

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

@Configuration
@Slf4j
public class KafkaErrorHandler {
    private final KafkaTemplate kafkaTemplate;

    private final String retryTopic;

    private final String deadLetterTopic;

    public KafkaErrorHandler(KafkaTemplate kafkaTemplate,
                             @Value("${app.topics.retry}") String retryTopic,
                             @Value("${app.topics.dlq}")String deadLetterTopic) {
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

//        var recoverer = new DeadLetterPublishingRecoverer(template,
//                (r, e) -> {
//                    if (e instanceof FooException) {
//                        return new TopicPartition(r.topic() + ".Foo.failures", r.partition());
//                    }
//                    else {
//                        return new TopicPartition(r.topic() + ".other.failures", r.partition());
//                    }
//                });


        return errorHandler;
    }

    public DeadLetterPublishingRecoverer publishingRecoverer(){

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishing Recoverer : {} ", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    }
                    else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                });

        return recoverer;

    }

}