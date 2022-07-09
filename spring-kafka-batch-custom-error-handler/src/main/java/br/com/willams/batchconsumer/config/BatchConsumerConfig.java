package br.com.willams.batchconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@Slf4j
public class BatchConsumerConfig {

    @Autowired
    KafkaProperties kafkaProperties;

    @Bean
    @ConditionalOnMissingBean(name = "batchKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> batchKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.kafkaProperties.buildConsumerProperties())));
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    public DefaultErrorHandler errorHandler() {

        var exceptiopnToIgnorelist = List.of(
                IllegalArgumentException.class
        );

        var defaultErrorHandler =  new DefaultErrorHandler((record, exception) -> {
            log.info("Executing Recovery Code here for the exception: {} and the failed record is {} ",
                    exception.getMessage(), record);
        }, new FixedBackOff(5_000L, 2L));
        //.addNotRetryableExceptions(exceptiopnToIgnorelist);
        exceptiopnToIgnorelist.forEach(defaultErrorHandler::addNotRetryableExceptions);

        defaultErrorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt)
        );

        return defaultErrorHandler;
    }
}