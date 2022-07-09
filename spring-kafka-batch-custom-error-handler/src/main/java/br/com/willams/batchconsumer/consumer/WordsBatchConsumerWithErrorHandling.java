package br.com.willams.batchconsumer.consumer;

import br.com.willams.batchconsumer.service.WordsService;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class WordsBatchConsumerWithErrorHandling {
    private WordsService wordsService;

    public WordsBatchConsumerWithErrorHandling(WordsService wordsService) {
        this.wordsService = wordsService;
    }

    @KafkaListener(
            topics = {"words-batch"},
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void onMessage(List<ConsumerRecord<String, String>> consumerRecords) {

        log.info("Total number of consumer Records : {} " , consumerRecords.size());
        consumerRecords.forEach((record) -> {
            log.info("Consumed Record is : {} ", record.value());
            try{
                wordsService.processWords(record.value());
            }catch(Exception e){
                log.error("Exception Observed");
               throw new BatchListenerFailedException("Failed to process Record : ", record);
            }
        });
    }
}