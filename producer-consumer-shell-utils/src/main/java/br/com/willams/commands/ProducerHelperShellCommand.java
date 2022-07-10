package br.com.willams.commands;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.util.concurrent.ExecutionException;

@ShellComponent
public class ProducerHelperShellCommand {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @ShellMethod("producer kafka message")
    public void producer(String msg) throws ExecutionException, InterruptedException {
        kafkaTemplate.send("words.batch", msg).get();
    }
}