package com.kafka.kafkaproducer.service;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaProducer {

    @Value("${spring.kafka.topic.name}")
    private String topicName;

    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private KafkaTemplate<String , String> kafkaTemplate;

    public void sendMessage() throws InterruptedException {

        while (true) {
            EventHandler eventHandler = new RealTimeDataChanges(kafkaTemplate, topicName);
            String url = "https://stream.wikimedia.org/v2/stream/recentchange";
            EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
            EventSource eventSource = builder.build();
            eventSource.start();
            // Read data for 10 seconds
            TimeUnit.SECONDS.sleep(10);
            // Close the event source
            eventSource.close();
            // Rest for another 10 seconds
            TimeUnit.SECONDS.sleep(10);
        }
    }
}
