package br.com.willams.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@Slf4j
public class KafkaConsumerConfig {
    private final KafkaProperties kafkaProperties;
    private final DefaultErrorHandler errorHandler;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties, DefaultErrorHandler errorHandler) {
        this.kafkaProperties = kafkaProperties;
        this.errorHandler = errorHandler;
    }

    @Bean
    @ConditionalOnMissingBean(name = "batchKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> batchKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<>();

        var defaultConsumerFactory = new DefaultKafkaConsumerFactory<>(this.kafkaProperties.buildConsumerProperties());
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable(() -> defaultConsumerFactory));

        factory.setBatchListener(true);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}