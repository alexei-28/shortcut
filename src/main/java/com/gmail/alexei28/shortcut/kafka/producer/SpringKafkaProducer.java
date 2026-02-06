package com.gmail.alexei28.shortcut.kafka.producer;

import static com.gmail.alexei28.shortcut.kafka.configuration.AppConfiguration.TOPIC_NAME;

import com.gmail.alexei28.shortcut.kafka.configuration.AppConfiguration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Spring Kafka producer that sends messages to the topic defined in {@link AppConfiguration}.
 * Messages are sent every 2 seconds with a random key and value. The producer uses Spring's
 * KafkaTemplate for sending messages and logs the results of each send operation, including any
 * exceptions that may occur.
 */
@Component
public class SpringKafkaProducer {
  private static final Random random = new Random();
  private final KafkaTemplate<String, String> kafkaTemplate;
  private static final Logger logger = LoggerFactory.getLogger(SpringKafkaProducer.class);

  public SpringKafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @Scheduled(fixedRate = 1_000)
  public void sendMessage() {
    String randomValue = String.valueOf(random.nextInt(1000));
    String key = "key-test-".concat(randomValue);
    String value = "value-test-".concat(randomValue);
    CompletableFuture<SendResult<String, String>> future =
        kafkaTemplate.send(TOPIC_NAME, key, value);
    future.whenComplete(
        (sendResult, ex) -> {
          if (ex == null) {
            logger.debug("sendMessage, sendResult: {}", sendResult);
          } else {
            logger.error("sendMessage, Error", ex);
          }
        });
  }
}
