package br.com.willams.commands;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@ShellComponent
@Slf4j
public class ProducerHelperShellCommand {

    @Autowired
    private KafkaTemplate kafkaTemplate;


    @ShellMethod("producer kafka message")
    public void producer(String topic, String msg) throws ExecutionException, InterruptedException {
        kafkaTemplate.send(topic, msg).get();
    }

    @ShellMethod("producer kafka message")
    public void prodrand(String topic, int nr) throws ExecutionException, InterruptedException {
        for(int i = 0; i < nr; i++) {
            sendMessages(topic, i + " " + UUID.randomUUID().toString());
        }
    }

    @ShellMethod("get last offset")
    public void lastoff(String topic) throws ExecutionException, InterruptedException {

        Map<Integer, Long> endingOffsets = getEndingOffsets("localhost:9092", topic);
        for (var entry : endingOffsets.entrySet())
            System.out.println("For partition: " + entry.getKey() + ", last offset is: " + entry.getValue());
    }




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

    @SuppressWarnings("unchecked")
    public Map<Integer, Long> getEndingOffsets(String kafkaBrokers, String topic) {
        Map<Integer, Long> retval = new HashMap<>();
        KafkaConsumer consumer = buildConsumer(kafkaBrokers);
        try {
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            List<PartitionInfo> partitionInfos = topics.get(topic);
            if (partitionInfos == null) {
                log.warn("Partition information was not found for topic {}", topic);
            } else {
                Collection<TopicPartition> partitions = new ArrayList<>();
                for (PartitionInfo partitionInfo : partitionInfos) {
                    partitions.add(new TopicPartition(topic, partitionInfo.partition()));
                }
                Map<TopicPartition, Long> endingOffsets = consumer.endOffsets(partitions);
                for (TopicPartition partition : endingOffsets.keySet()) {
                    retval.put(partition.partition(), endingOffsets.get(partition));
                }
            }
        } finally {
            consumer.close();
        }
        return retval;
    }

    private long diffOffsets(Map<TopicPartition, Long> beginning, Map<TopicPartition, Long> ending) {
        long retval = 0;
        for (TopicPartition partition : beginning.keySet()) {
            Long beginningOffset = beginning.get(partition);
            Long endingOffset = ending.get(partition);
            System.out.println("Begin = " + beginningOffset + ", end = " + endingOffset + " for partition " + partition);
            if (beginningOffset != null && endingOffset != null) {
                retval += (endingOffset - beginningOffset);
            }
        }
        return retval;
    }

    private KafkaConsumer buildConsumer(String kafkaBrokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        return new KafkaConsumer(props);
    }
}