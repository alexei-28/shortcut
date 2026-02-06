package com.gmail.alexei28.shortcut.kafka.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Spring configuration class that defines Kafka topics for the application. It creates a topic
 * named "test_topic" with 3 partitions and the default replication factor (1).
 */
@Configuration
public class AppConfiguration {
  public static final String TOPIC_NAME = "test_topic";
  private static final Logger logger = LoggerFactory.getLogger(AppConfiguration.class);

  @Bean
  public NewTopic crateTestTopic1() {
    logger.debug("Creating topic {} with 1 partition and replicas 3", TOPIC_NAME);
    return TopicBuilder.name(TOPIC_NAME).partitions(1).replicas(3).build();
  }
}
