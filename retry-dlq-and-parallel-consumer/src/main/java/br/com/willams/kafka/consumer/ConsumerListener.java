package br.com.willams.kafka.consumer;

import br.com.willams.aop.TrackExecutionTime;
import br.com.willams.kafka.util.TopicName;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class ConsumerListener {
    private final ConsumerProcessor consumerProcessor;

    public static final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

    private final TopicName topicName;

    @Bean(name = "processAsyncExecutor")
    public Executor asyncExecutor() {
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(10);
        executor.initialize();
        return executor;
    }

    @Async("processAsyncExecutor")
    private CompletableFuture<Boolean> processAsync(String value) {
        return CompletableFuture.completedFuture(consumerProcessor.process(value));
    }

    @Bean
    TopicName topicName() {
        return new TopicName("words.batch", "retry", "dlq", 3);
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"#{topicName.getMainTopicName()}"}, containerFactory = "kafkaListenerContainerFactory", batch = "true")
    public void receive(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment ack) {

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
    @KafkaListener(topicPattern = "*.#{topicName.getRetrySuffix()}-*", containerFactory = "kafkaListenerContainerFactory")
    public void retry0(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        if (!shouldProcess(consumerRecord, 5, ack)) return;
        consumerProcessor.process(consumerRecord);
        ack.acknowledge();
    }

    @TrackExecutionTime
    @KafkaListener(topics = "", containerFactory = "kafkaListenerContainerFactory", batch = "true")
    public void retry(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment acknowledgment) {
        log.info("Number of records to process in this batch: {}", consumerRecords.size());
        var futures = consumerRecords.stream()
                .peek(this::logIt)
                .map(ConsumerRecord::value)
                .map(this::processAsync)
                .toArray(size -> new CompletableFuture[size]);

        CompletableFuture.allOf(futures).join();

        acknowledgment.acknowledge();
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"words.batch.retry-1"}, containerFactory = "kafkaListenerContainerFactory")
    public void retry1(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        if (!shouldProcess(consumerRecord, 10, ack)) return;
        consumerProcessor.process(consumerRecord);
        ack.acknowledge();
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"words.batch.retry-2"}, containerFactory = "kafkaListenerContainerFactory")
    public void retry2(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        if (!shouldProcess(consumerRecord, 15, ack)) return;
        consumerProcessor.process(consumerRecord);
        ack.acknowledge();
    }

    @TrackExecutionTime
    @KafkaListener(topics = {"words.batch.retry-3"}, containerFactory = "kafkaListenerContainerFactory")
    public void retry3(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        if (!shouldProcess(consumerRecord, 20, ack)) return;
        consumerProcessor.process(consumerRecord);
        ack.acknowledge();
    }


    private CompletableFuture<Void> processAsync(ConsumerRecord<String, String> consumerRecord) {
        return CompletableFuture
                .runAsync(() -> consumerProcessor.process(consumerRecord), executor)
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
}