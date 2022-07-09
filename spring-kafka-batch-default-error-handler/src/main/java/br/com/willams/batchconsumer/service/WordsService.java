package br.com.willams.batchconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class WordsService {

    public void processWords(String word) {
        if (word.equals("error")) {
            throw new IllegalStateException("Value Not Allowed");
        }
        log.info("Processed the word {} successfully!", word);
    }
}