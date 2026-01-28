package com.gmail.alexei28.shortcut.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
  private KafkaProducer<String, String> kafkaProducer;

  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  public Producer() {
    initProducer();
  }

  private void initProducer() {
    logger.debug("initProducer");
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProducer = new KafkaProducer<>(kafkaProps);
  }

  private void sendMessage() {
    logger.debug("sendMessage");
    /*-
        Производитель(producer) получает на входе объекты ProducerRecord, так что начнем с создания такого объекта.
        У класса ProducerRecord есть несколько конструкторов, которые обсудим позднее.
        В данном случае мы имеем дело с конструктором, принимающим на входе строковое значение — название топика, в который
        отправляются данные, и отсылаемые в Kafka ключ и значение (тоже строки).
        Типы ключа и значения должны соответствовать нашим объектам key serialize и value serializer.
    */
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
    try {
      /*-
         Для отправки объекта типа ProducerRecord используем метод send() объекта kafkaProducer.
         Сообщение помещается в буфер и отправляется брокеру в отдельном потоке.
         Метод send() возвращает объект класса java.util.concurrent.Future, включающий RecordMetadata,
         но поскольку мы просто игнорируем возвращаемое значение, то никак не можем узнать, успешно ли было отправлено сообщение.
         Такой способ отправки сообщений можно использовать, только если потерять сообщение вполне допустимо.
      */
      kafkaProducer.send(producerRecord);
    } catch (Exception e) {
      logger.error("Error sending message to Kafka: {}", e.getMessage(), e);
    }
  }
}
