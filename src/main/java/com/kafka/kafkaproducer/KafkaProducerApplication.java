package com.kafka.kafkaproducer;

import com.kafka.kafkaproducer.service.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
public class KafkaProducerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerApplication.class, args);
	}

	@Autowired
	private KafkaProducer producer;

	@Override
	public void run(String... args) throws Exception {
		producer.sendMessage();
	}
}
