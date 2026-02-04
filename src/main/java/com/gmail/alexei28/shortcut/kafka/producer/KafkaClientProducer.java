package com.gmail.alexei28.shortcut.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClientProducer {
  private final Properties kafkaProps = new Properties();
  private static final String TOPIC = "sandbox";
  private static final String MY_KEY = "MyKey";
  private static final int PARTITION_0 = 0;
  private static final int PARTITION_1 = 1;
  private static final int PARTITION_2 = 2;

  private static final Logger logger = LoggerFactory.getLogger(KafkaClientProducer.class);

  private void initProducerConfig() {
    kafkaProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092,localhost:49092");
    // Поскольку мы планируем использовать строки для ключа и значения сообщения, воспользуемся
    // встроенным типом StringSerializer.
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  }

  private void initProducerConfigWithTransactions() {
    kafkaProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092,localhost:49092");
    // Поскольку мы планируем использовать строки для ключа и значения сообщения, воспользуемся
    // встроенным типом StringSerializer.
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // Устанавливаем уникальный идентификатор транзакции для нашего производителя.
    kafkaProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
  }

  public void sendMessage(String message) {
    initProducerConfig();
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
      logger.debug("sendMessage, message sending: {}", message);
      /*-
       Method send is asynchronous by default.
       Для использования функций обратного вызова нам понадобится класс, реализующий интерфейс org.apache.kafka.clients.producer.Callback,
       включающий одну-единственную функцию onCompletion().
       Обратные вызовы выполняются в главном потоке производителя.
       Это гарантирует, что, когда мы отправляем два сообщения в один и тот же раздел одно за другим,
       их обратные вызовы будут выполняться в том же порядке, в котором
       мы их отправили. Но это означает также, что обратный вызов должен выполняться достаточно быстро,
       чтобы не задерживать производителя и не мешать
       отправке других сообщений. Не рекомендуется выполнять блокирующие операции внутри обратного вызова.
       Вместо этого следует использовать другой поток для одновременного выполнения любой блокирующей операции.

       Производитель(producer) получает на входе объекты ProducerRecord.
       В данном случае мы имеем дело с конструктором, принимающим на входе строковое значение — название топика, в который
       отправляются данные, и отсылаемые в Kafka ключ и значение (тоже строки).
       Типы ключа и значения должны соответствовать нашим объектам key serialize и value serializer.
      */
      Future<RecordMetadata> future =
          producer.send(
              new ProducerRecord<>(TOPIC, PARTITION_0, MY_KEY, message),
              (metadata, exception) ->
                  logger.debug(
                      "sendMessage, callback_called_with_metadata: {}, exception: {}",
                      metadata,
                      exception));
      /*-
        Method future.get() is blocking and waits for the result.
        Используем метод Future.get() для ожидания ответа от Kafka.
        Если необходимо убедиться в успешной отправке сообщения, можно использовать метод get() объекта Future,
        возвращаемого методом send(). Этот метод блокирует вызывающий поток до тех пор, пока сообщение не будет
        отправлено и подтверждено брокером Kafka.
        Этот метод генерирует исключение в случае неудачи отправки записи в Kafka. При отсутствии ошибок мы получим объект
        RecordMetadata, из которого можно узнать смещение, соответствующее записанному сообщению, и другие метаданные.
      */
      RecordMetadata recordMetadata = future.get(); // synchronous wait for send to complete
      logger.debug("sendMessage, message sent to: {}", recordMetadata);
      logger.debug("sendMessage, message sent successfully");
    } catch (Exception e) {
      /*-
        Хотя ошибки, возможные при отправке сообщений брокерам Kafka или возникающие в самих брокерах, игнорируются,
        при появлении в производителе ошибки перед отправкой сообщения в Kafka вполне может быть сгенерировано исключение.
        К примеру, это может быть исключение SerializationException при неудачной сериализация сообщения, BufferExhaustedException или
        TimeoutException при переполнении буфера, InterruptException при сбое отправляющего потока.
      */
      logger.error("sendMessage, Error sending message to Kafka: {}", e.getMessage(), e);
    }
  }

  // Отправка нескольких сообщений в рамках транзакции.
  // Все сообщения будут либо успешно записаны, либо не записаны вовсе.
  public void sendMessagesWithTransaction(String message) {
    logger.debug("sendMessagesWithTransaction, message sending in transaction: {}", message);
    initProducerConfigWithTransactions();
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
      // Инициализируем транзакции.
      producer.initTransactions();

      // Начинаем новую транзакцию.
      producer.beginTransaction();
      // Отправляем несколько сообщений в рамках транзакции.
      producer.send(new ProducerRecord<>(TOPIC, PARTITION_0, MY_KEY, message + " tx_0"));
      producer.send(new ProducerRecord<>(TOPIC, PARTITION_0, MY_KEY, message + " tx_1"));
      producer.send(new ProducerRecord<>(TOPIC, PARTITION_0, MY_KEY, message + " tx_2"));

      // Фиксируем транзакцию.
      producer.commitTransaction();
      logger.debug("sendMessagesWithTransactional, Transaction committed successfully");
    }
  }
}
