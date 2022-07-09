package br.com.willams.kafkaconsumer.consumer;

import br.com.willams.kafkaconsumer.aop.TrackExecutionTime;
import br.com.willams.kafkaconsumer.service.ConsumerProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class ConsumerListener {
    private ConsumerProcessorService consumerProcessorService;

    public ConsumerListener(ConsumerProcessorService consumerProcessorService) {
        this.consumerProcessorService = consumerProcessorService;
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"${app.topics.main}"}, containerFactory = "batchKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecords<String, String> consumerRecords, Acknowledgment ack) {

        log.info("Total number of consumer Records : {} ", consumerRecords.count());

        consumerRecords.forEach((record) -> {
            log.info("Consumed Record is : {} partion is {} offset is {} ", record.value(), record.partition(), record.offset());

            try {
                consumerProcessorService.processWords(record.value());
            } catch (Exception e) {
                log.error("Exception Observed");
                throw new BatchListenerFailedException("Failed to process Record : ", record);
            }

        });

        assert ack != null;

        ack.acknowledge();
    }

//    @KafkaListener(id = "list", topics = "myTopic", containerFactory = "batchFactory")
//    public void listen1(List<String> list,
//                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<Integer> keys,
//                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
//                       @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
//                       @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
//
//    }
}