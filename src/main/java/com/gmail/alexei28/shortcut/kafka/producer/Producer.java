package com.gmail.alexei28.shortcut.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
  private KafkaProducer<String, String> kafkaProducer;
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  public Producer() {
    init();
  }

  private void init() {
    logger.debug("initProducer");
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
    // Поскольку мы планируем использовать строки для ключа и значения сообщения, воспользуемся
    // встроенным типом StringSerializer.
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    // Создаем новый производитель, задавая подходящие типы ключа и значениями передавая в
    // конструктор объект Properties
    kafkaProducer = new KafkaProducer<>(kafkaProps);
  }

  public void sendMessage() {
    logger.debug("sendMessage");
    /*-
        Производитель(producer) получает на входе объекты ProducerRecord.
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
      /*-
        Хотя ошибки, возможные при отправке сообщений брокерам Kafka или возникающие в самих брокерах, игнорируются,
        при появлении в производителе ошибки перед отправкой сообщения в Kafka вполне может быть сгенерировано исключение.
        К примеру, это может быть исключение SerializationException при неудачной сериализация сообщения, BufferExhaustedException или
        TimeoutException при переполнении буфера, InterruptException при сбое отправляющего потока.
      */
      logger.error("Error sending message to Kafka: {}", e.getMessage(), e);
    }
  }

  public void sendSyncMessage() {
    logger.debug("sendSyncMessage");
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
    try {
      /*-
        Используем метод Future.get() для ожидания ответа от Kafka.
        Если необходимо убедиться в успешной отправке сообщения, можно использовать метод get() объекта Future,
        возвращаемого методом send(). Этот метод блокирует вызывающий поток до тех пор, пока сообщение не будет
        отправлено и подтверждено брокером Kafka.
        Этот метод генерирует исключение в случае неудачи отправки записи в Kafka. При отсутствии ошибок мы получим объект
        RecordMetadata, из которого можно узнать смещение, соответствующее записанному сообщению, и другие метаданные.
      */
      Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
      RecordMetadata recordMetadata = future.get();
      logger.debug(
          "sendSyncMessage, to recordMetadata: {}-{} with offset {}",
          recordMetadata.topic(),
          recordMetadata.partition(),
          recordMetadata.offset());
    } catch (Exception e) {
      /*-
         Если перед отправкой или во время отправки записи в Kafka возникли ошибки, нас будет ожидать исключение.
         В этом случае просто выводим информацию о нем.
      */
      logger.error("sendSyncMessage, Error sending message to Kafka: {}", e.getMessage(), e);
    }
  }

  public void sendAsyncMessage() {
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA");

    /*-
     Для использования функций обратного вызова нам понадобится класс, реализующий интерфейс org.apache.kafka.clients.producer.Callback,
     включающий одну-единственную функцию onCompletion().
     Обратные вызовы выполняются в главном потоке производителя.
     Это гарантирует, что, когда мы отправляем два сообщения в один и тот же раздел одно за другим,
     их обратные вызовы будут выполняться в том же порядке, в котором
     мы их отправили. Но это означает также, что обратный вызов должен выполняться достаточно быстро,
     чтобы не задерживать производителя и не мешать
     отправке других сообщений. Не рекомендуется выполнять блокирующие операции внутри обратного вызова.
     Вместо этого следует использовать другой поток для одновременного выполнения любой блокирующей операции.
    */
    kafkaProducer.send(
        producerRecord,
        new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            if (exception != null) {
              // Если Kafka вернет ошибку, в функцию onCompletion() попадет непустое исключение.
              logger.error(
                  "sendASyncMessage, Error sending message to Kafka: {}",
                  exception.getMessage(),
                  exception);
            } else {
              logger.debug(
                  "sendASyncMessage, to recordMetadata: {}-{} with offset {}",
                  recordMetadata.topic(),
                  recordMetadata.partition(),
                  recordMetadata.offset());
            }
          }
        });
  }
}
