package br.com.willams.kafkaconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    public void sendMessages(String topic, Object model) {
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, model);

        future.addCallback(new ListenableFutureCallback() {
            @Override
            public void onSuccess(Object result) {

            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Unable to send message - data {}", ex);
                throw new RuntimeException(ex);
            }
        });

        try {
            future.get(30, TimeUnit.SECONDS);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
