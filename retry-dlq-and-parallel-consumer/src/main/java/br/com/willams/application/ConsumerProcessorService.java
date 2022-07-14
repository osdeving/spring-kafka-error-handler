package br.com.willams.application;

import br.com.willams.aop.TrackExecutionTime;
import br.com.willams.kafka.consumer.ConsumerProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerProcessorService implements ConsumerProcessor<String> {

    @TrackExecutionTime
    public boolean process(String word) {

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        String numberString = StringUtils.substringBefore(word, " ");
        int number = Integer.valueOf(numberString);
//
//        if (number % 2 == 0) {
//            if(consumerRecord.topic().contains("retry-0"))
//                kafkaProducerService.sendMessages("words.batch.retry-1", word);
//            else if(consumerRecord.topic().contains("retry-1"))
//                kafkaProducerService.sendMessages("words.batch.retry-2", word);
//            else if(consumerRecord.topic().contains("retry-2"))
//                kafkaProducerService.sendMessages("words.batch.retry-3", word);
//            else if(consumerRecord.topic().contains("retry-3"))
//                kafkaProducerService.sendMessages("words.batch.dlq", word);
//            else
//                kafkaProducerService.sendMessages("words.batch.retry-0", word);
//
//        }

        log.info("Processed the word {} successfully!", word);

        return true;
    }
}