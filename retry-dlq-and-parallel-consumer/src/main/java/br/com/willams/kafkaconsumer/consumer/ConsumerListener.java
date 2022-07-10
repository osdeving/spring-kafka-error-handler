package br.com.willams.kafkaconsumer.consumer;

import br.com.willams.kafkaconsumer.aop.TrackExecutionTime;
import br.com.willams.kafkaconsumer.service.ConsumerProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ConsumerListener {
    private final ConsumerProcessorService consumerProcessorService;

    private static Executor executor;

    public ConsumerListener(ConsumerProcessorService consumerProcessorService) {
        this.consumerProcessorService = consumerProcessorService;

        if(this.executor == null) {
            this.executor = Executors.newFixedThreadPool(100,
                    task -> {
                Thread t = new Thread(task);
                t.setDaemon(true);
                return t;
                    });
        }
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"${app.topics.main}"}, containerFactory = "batchKafkaListenerContainerFactory")
    public void onMessage(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment ack) {

        log.info("Total number of consumer Records : {} ", consumerRecords.size());

        var futures = consumerRecords.stream()
                .peek(this::logIt)
                .map(ConsumerRecord::value)
                .map(this::processAsync)
                .toArray(size -> new CompletableFuture[size]);

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

    private CompletableFuture<Void> processAsync(String value) {
        return CompletableFuture
                .runAsync(() -> consumerProcessorService.processWords(value), executor)
                .orTimeout(1, TimeUnit.MINUTES);

    }

    private ConsumerRecord<String, String> logIt(ConsumerRecord<String, String> consumerRecord) {
        log.info("# ConsumerListener::receive - Record details: topic={}, partition={}, offset={}, key={}, timestamp={}, value={}",
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key(),
                consumerRecord.timestamp(),
                consumerRecord.value());

        return consumerRecord;
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