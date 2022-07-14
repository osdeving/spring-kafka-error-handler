package br.com.willams.kafka.producer;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {
    private static final String KAFKA_ERROR = "Kafka unable to send message - data={} - execpiont={}";
    private final KafkaTemplate<String, Object> template;

    public void send(String topic, Object model) {
        ListenableFuture<SendResult<String, Object>> future = template.send(topic, model);

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("[{}] Kafka sent message - message={} with offset={}", topic, model, result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error(KAFKA_ERROR, model, ex);
                throw new RuntimeException(ex);
            }
        });

        try {
            future.get(1, TimeUnit.MINUTES);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}