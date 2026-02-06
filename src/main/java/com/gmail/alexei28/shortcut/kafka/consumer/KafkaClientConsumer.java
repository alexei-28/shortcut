package com.gmail.alexei28.shortcut.kafka.consumer;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClientConsumer {
  private KafkaConsumer<String, String> kafkaConsumer;
  private final Map<String, Integer> custCountryMap = new HashMap<>();
  private final Properties kafkaProps = new Properties();
  private static final String TOPIC = "test-topic";
  private static final String MY_KEY = "MyKey";
  private static final int PARTITION_0 = 0;
  private static final int PARTITION_1 = 1;
  private static final int PARTITION_2 = 2;
  private static final Logger logger = LoggerFactory.getLogger(KafkaClientConsumer.class);

  private void initConsumerConfig() {
    kafkaProps.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
    // Поскольку мы планируем использовать строки для ключа и значения сообщения, воспользуемся
    // встроенным типом StringSerializer.
    kafkaProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    kafkaProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group-id");
  }

  /** Потребление сообщений из топика Kafka. */
  public void consumeMessages() {
    logger.debug("consumeMessages, start consuming messages");
    initConsumerConfig();
    /*-
       Метод poll() при вызове возвращает записанные в Kafka данные, еще не прочитанные потребителями из нашей группы.
       Потребители должны опрашивать Kafka, иначе их сочтут неработающими, а разделы, откуда они получают данные,
       будут переданы другим потребителям группы. Передаваемый нами в метод poll() параметр представляет
       собой длительность ожидания и определяет, сколько времени будет длиться блокировка в случае недоступности
       данных в буфере потребителя. Если этот параметр равен 0 или если уже имеются записи, возврат из метода poll()
       произойдет немедленно, в противном случае он будет ожидать в течение указанного числа миллисекунд.
          Если poll() не вызывается дольше, чем значение max.poll.interval.ms, потребитель станет считаться
       мертвым и будет вычеркнут из группы потребителей, поэтому избегайте действий,
       которые могут блокировать на непредсказуемые интервалы времени внутри цикла опроса.
       - max.poll.records - максимальное количество записей(consumerRecords), возвращаемых в одном вызове poll().
       Используйте его для управления количеством данных (но не размером данных), которые ваше приложение должно обработать
       за одну итерацию цикла опроса.
          При возврате методом KafkaConsumer.poll() объекта ConsumerRecords объект записи будет занимать
       не более max.partition.fetch.bytes на каждый назначенный потребителю раздел.
       Настоятельно рекомендуется использовать свойство fetch.max.bytes, если у вас нет особых причин пытаться
       обрабатывать одинаковые объемы данных из каждого раздела.
       Метод poll() возвращает список записей, каждая из которых содержит топик и раздел, из которого она поступила,
       смещение записи в разделы и, конечно, ключ и значение записи. Обычно по списку проходят в цикле,
       и записи обрабатываются по отдельности.
    */
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
      consumer.subscribe(Pattern.compile(TOPIC), new MyConsumerRebalanceListener());
      ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(5000));
      StreamSupport.stream(consumerRecords.spliterator(), false)
          .forEach(
              consumerRecord ->
                  logger.debug(
                      "consumeMessages, consumerRecord: topic: {}, partition: {}, offset: {},  key {}, value : {}",
                      consumerRecord.topic(),
                      consumerRecord.partition(),
                      consumerRecord.offset(),
                      consumerRecord.key(),
                      consumerRecord.value()));
    }
  }

  static class MyConsumerRebalanceListener implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      logger.debug("onPartitionsRevoked, partitions: {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      logger.debug("onPartitionsAssigned, partitions: {}", partitions);
    }
  }
}
