package br.com.willams.kafkaconsumer.consumer;

import br.com.willams.kafkaconsumer.aop.TrackExecutionTime;
import br.com.willams.kafkaconsumer.service.ConsumerProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
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

        if (this.executor == null) {
            this.executor = Executors.newFixedThreadPool(100,
                    task -> {
                        Thread t = new Thread(task);
                        t.setDaemon(true);
                        return t;
                    });
        }
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"${app.topics.main}"}, containerFactory = "kafkaListenerContainerFactory", batch = "true")
    public void onMessage(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment ack) {

        log.info("Total number of consumer Records : {} ", consumerRecords.size());

        var futures = consumerRecords.stream()
                .peek(this::logIt)
                //.map(ConsumerRecord::value)
                .map(this::processAsync)
                .toArray(size -> new CompletableFuture[size]);

        CompletableFuture.allOf(futures).join();

        ack.acknowledge();
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"words.batch.retry-0"}, containerFactory = "kafkaListenerContainerFactory")
    public void retry0(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        if (!shouldProcess(consumerRecord, 5, ack)) return;
        consumerProcessorService.processWords(consumerRecord);
        ack.acknowledge();
    }



    @TrackExecutionTime
    @KafkaListener(topics = {"words.batch.retry-1"}, containerFactory = "kafkaListenerContainerFactory")
    public void retry1(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        if (!shouldProcess(consumerRecord, 10, ack)) return;
        consumerProcessorService.processWords(consumerRecord);
        ack.acknowledge();
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"words.batch.retry-2"}, containerFactory = "kafkaListenerContainerFactory")
    public void retry2(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        if (!shouldProcess(consumerRecord, 15, ack)) return;
        consumerProcessorService.processWords(consumerRecord);
        ack.acknowledge();
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"words.batch.retry-3"}, containerFactory = "kafkaListenerContainerFactory")
    public void retry3(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        if (!shouldProcess(consumerRecord, 20, ack)) return;
        consumerProcessorService.processWords(consumerRecord);
        ack.acknowledge();
    }


    private CompletableFuture<Void> processAsync(ConsumerRecord<String, String> consumerRecord) {
        return CompletableFuture
                .runAsync(() -> consumerProcessorService.processWords(consumerRecord), executor)
                .orTimeout(1, TimeUnit.MINUTES);

    }

    private boolean shouldProcess(ConsumerRecord<String, String> consumerRecord, int secondsToWait, Acknowledgment ack) {
        var now = Instant.now().toEpochMilli();
        var diff = now - consumerRecord.timestamp();
        var diffSeconds = diff > 1000 ? diff / 1000 : 0;

        if (diffSeconds < secondsToWait) {
            var nack = secondsToWait - diffSeconds;
            log.info("Fazendo NACK de {} segundos no tópico retry {}", nack, consumerRecord.topic());

            ack.nack(Duration.ofSeconds(nack));

            return true;
        }
        return false;
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