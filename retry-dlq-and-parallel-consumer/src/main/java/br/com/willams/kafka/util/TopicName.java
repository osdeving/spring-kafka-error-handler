package br.com.willams.kafka.util;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.List;


@Slf4j
@Getter
public class TopicName {
    private int index = 0;
    List<String> topics = new ArrayList<>();

    public TopicName(@Value("${app.topics.main}") String mainTopic, int numOfTries) {
        this(mainTopic, "retry", "dlq", numOfTries);
    }

    public TopicName(@Value("${app.topics.main}") String mainTopic) {
        this(mainTopic, "retry", "dlq", 3);
    }

    public TopicName(@Value("${app.topics.main}") String mainTopic, String retrySuffix, String dlqSuffix, int numOfTries) {
        topics.add(mainTopic);

        for(int i = 1; i < numOfTries - 1; i++) {
            topics.add(mainTopic + "." + retrySuffix + "-" + (i - 1));
        }

        topics.add(mainTopic + "." + dlqSuffix);
    }

    public String getDlqTopicName() {
        return topics.get(topics.size() - 1);
    }

    public String getMainTopicName() {
        return topics.get(0);
    }

    public String next() {
        index++;

        if(index >= topics.size()) {
            index = 0;
        }

        return topics.get(index);
    }
}