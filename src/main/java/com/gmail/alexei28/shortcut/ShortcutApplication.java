package com.gmail.alexei28.shortcut;

import com.gmail.alexei28.shortcut.kafka.consumer.KafkaClientConsumer;
import com.gmail.alexei28.shortcut.kafka.producer.KafkaClientProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ShortcutApplication {
  private static final Logger logger = LoggerFactory.getLogger(ShortcutApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(ShortcutApplication.class, args);

    logger.info("Application started successfully!");
    logger.info(
        "Java version: {}, Java vendor: {}",
        System.getProperty("java.version"),
        System.getProperty("java.vendor"));

    // testKafkaClient();
  }

  private static void testKafkaClient() {
    new KafkaClientProducer().sendMessage("Hello from Kafka Java client!");
    new KafkaClientProducer().sendMessagesWithTransaction("Hello from Kafka Java client!");
    new KafkaClientConsumer().consumeMessages();
  }
}
