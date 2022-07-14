package br.com.willams.kafka.consumer.impl;

import br.com.willams.kafka.consumer.ConsumerProcessor;
import br.com.willams.kafka.consumer.SendToRetryService;
import br.com.willams.kafka.producer.KafkaProducerService;
import br.com.willams.kafka.util.TopicName;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.BackOffExecution;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;

@Service
@Slf4j
@RequiredArgsConstructor
public class SendToRetryServiceImpl implements SendToRetryService<ConsumerRecord<String, String>> {

    private final KafkaProducerService kafkaProducerService;

    private final TopicName topicName;

    private final ExponentialBackOffWithMaxRetries backoff;

    @Override
    public void processRetry(ConsumerRecord<String, String> consumerRecord, ConsumerProcessor consumerProcessor) {
        logIt(consumerRecord);

        BackOffExecution backOffExecution = backoff.start();

        long l = backOffExecution.nextBackOff();

        try {
            consumerProcessor.process(consumerRecord.value());
        } catch (Exception ex) {
            kafkaProducerService.send(topicName.next(), consumerRecord.value());
        }
    }

    private void logIt(ConsumerRecord<String, String> record) {
        String LOG_FORMAT = "# SendToRetryService::processRetry - Record details: topic={}, creationTime={}, receivedTime={}, partition={}, offset={}, key={},  value={}";

        Instant instant = Instant.ofEpochMilli(record.timestamp());
        LocalTime creationLocalTime = LocalTime.ofInstant(instant, ZoneId.systemDefault());
        LocalTime receivedLocalTime = LocalTime.now();

        log.info(LOG_FORMAT, record.topic(), creationLocalTime, receivedLocalTime, record.partition(), record.offset(), record.key(), record.value());
    }

}