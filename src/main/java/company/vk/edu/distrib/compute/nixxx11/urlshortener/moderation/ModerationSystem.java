package company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModerationSystem {
  private static final Logger LOG = LoggerFactory.getLogger(ModerationSystem.class);

  public static final String CREATED_TOPIC = "links.created";
  public static final String MODERATED_TOPIC = "links.moderated";
  public static final String MODERATED_STATUS = "OK";

  private final String bootstrapServers;
  private final String groupId;
  private final Consumer<String> onLinkDeleted;
  private KafkaProducer<String, String> producer;
  private final ExecutorService executorService = Executors.newFixedThreadPool(2);

  public ModerationSystem(final String bootstrapServers, final String groupId, final Consumer<String> onLinkDeleted) {
    this.bootstrapServers = bootstrapServers;
    this.groupId = groupId;
    this.onLinkDeleted = onLinkDeleted;
  }

  public void start() {
    final Properties consumerProperties = prepareConsumerProperties(bootstrapServers, groupId);
    executorService.submit(() -> runKafkaConsumer(onLinkDeleted, consumerProperties));
    final Properties producerProperties = prepareProducerProperties(bootstrapServers);
    producer = new KafkaProducer<>(producerProperties);
  }

  public static Properties prepareConsumerProperties(final String bootstrapServers, final String groupId) {
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  public static Properties prepareProducerProperties(final String bootstrapServers) {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    return properties;
  }

  private static void runKafkaConsumer(
      final Consumer<String> onLinkDeleted,
      final Properties consumerProperties
  ) {
    try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
      consumer.subscribe(List.of(MODERATED_TOPIC));
      while (true) {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (final ConsumerRecord<String, String> record : records) {
          final String key = record.key();
          final String value = record.value();
          if (value.equals(MODERATED_STATUS)) {
            continue;
          }
          LOG.info("Got moderation issue for '{}': {}", key, value);
          onLinkDeleted.accept(key);
        }
      }
    }
  }

  public void linkCreated(final String key, final String value) {
    executorService.submit(() -> producer.send(new ProducerRecord<>(CREATED_TOPIC, key, value)));
  }

  public void stop() {
    try {
      if (producer != null) {
        producer.close();
      }
    } finally {
      executorService.shutdownNow();
    }
  }
}
