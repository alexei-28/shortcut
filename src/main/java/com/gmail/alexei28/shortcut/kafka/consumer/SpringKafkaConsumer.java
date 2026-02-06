package com.gmail.alexei28.shortcut.kafka.consumer;

import com.gmail.alexei28.shortcut.kafka.producer.SpringKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Spring Kafka consumer that listens to the topic defined in {@link SpringKafkaProducer}. It has a
 * method annotated with @KafkaListener that receives messages from the topic and logs the received
 * messages. The method can be configured to receive the entire ConsumerRecord or just the payload
 * and partition information, depending on the desired level of detail in the logs.
 */
@Component
public class SpringKafkaConsumer {
  private static final Logger logger = LoggerFactory.getLogger(SpringKafkaConsumer.class);

  @KafkaListener(topics = "test_topic")
  public void receiveMessage(ConsumerRecord<String, String> consumerRecord) {
    logger.debug("receiveMessage, consumerRecord: {}", consumerRecord);
  }
}
