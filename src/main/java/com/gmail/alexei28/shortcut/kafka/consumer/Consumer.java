package com.gmail.alexei28.shortcut.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {
  private KafkaConsumer<String, String> consumer;
  private Map<String, Integer> custCountryMap = new HashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

  public Consumer() {
    init();
  }

  private void init() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "broker1:9092,broker2:9092");
    props.put("group.id", "CountryCounter"); // optional
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumer = new KafkaConsumer<>(props);
    // Просто создаем список, содержащий один элемент — название топика /customerCountries.
    consumer.subscribe(Collections.singletonList("customerCountries"));
  }

  private void consumeMessages() {
    /*-
       Бесконечный цикл.
       Потребители обычно представляют собой работающие в течение длительного времени приложения,
       непрерывно опрашивающие Kafka на предмет дополнительных данных.
    */
    while (true) {
      /*-
         Потребители должны опрашивать Kafka, иначе их сочтут неработающими, а разделы, откуда они получают данные,
         будут переданы другим потребителям группы. Передаваемый нами в метод poll() параметр представляет
         собой длительность ожидания и определяет, сколько времени будет длиться блокировка в случае недоступности
         данных в буфере потребителя. Если этот параметр равен 0 или если уже имеются записи, возврат из метода poll()
         произойдет немедленно, в противном случае он будет ожидать в течение указанного числа миллисекунд.
      */
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

      /*-
          Метод poll() возвращает список записей, каждая из которых содержит топик и раздел, из которого она поступила,
          смещение записи в разделы и, конечно, ключ и значение записи. Обычно по списку проходят в цикле,
          и записи обрабатываются по отдельности.
      */
      for (ConsumerRecord<String, String> record : records) {
        logger.debug(
            "consumeMessages, topic = {}, partition = {}, offset = {}. key = {}, value = {}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            record.value());
        int updatedCount = 1;
        if (custCountryMap.containsKey(record.value())) {
          updatedCount = custCountryMap.get(record.value()) + 1;
        }
        custCountryMap.put(record.value(), updatedCount);
        JSONObject json = new JSONObject(custCountryMap);

        /*-
            Обработка обычно заканчивается записью результата в хранилище данных или обновлением сохраненной записи.
            Цель состоит в ведении текущего списка покупателей из каждого округа, так что мы обновляем хеш-таблицу и выводим
            результат в виде JSON. В более реалистичном примере результаты обновлений сохранялись бы в хранилище данных.
        */
        logger.debug("consumeMessages, json = {}", json);
      }
    }
  }
}
