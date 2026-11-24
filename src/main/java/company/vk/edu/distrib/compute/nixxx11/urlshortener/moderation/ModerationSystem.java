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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModerationSystem {
  private static final Logger LOG = LoggerFactory.getLogger(ModerationSystem.class);

  public static final String CREATED_TOPIC = "links.created";
  public static final String MODERATED_TOPIC = "links.moderated";
  public static final String MODERATED_STATUS = "OK";

  private final String bootstrapServers;
  private final String groupId;
  private final Consumer<String> onModeration;
  private KafkaConsumer<String, String> consumer;
  private KafkaProducer<String, String> producer;
  private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
  private final ExecutorService producerExecutor = Executors.newSingleThreadExecutor();

  public ModerationSystem(final String bootstrapServers, final String groupId, final Consumer<String> onModeration) {
    this.bootstrapServers = bootstrapServers;
    this.groupId = groupId;
    this.onModeration = onModeration;
  }

  public void start() {
    consumer = createConsumer(bootstrapServers, groupId);
    consumer.subscribe(List.of(MODERATED_TOPIC));
    consumerExecutor.submit(this::consumerLoop);

    producer = createProducer(bootstrapServers);
  }

  public static KafkaConsumer<String, String> createConsumer(final String bootstrapServers, final String groupId) {
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(properties);
  }

  public static KafkaProducer<String, String> createProducer(final String bootstrapServers) {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(properties);
  }

  private void consumerLoop() {
    try {
      while (true) {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (final ConsumerRecord<String, String> record : records) {
          final String key = record.key();
          final String value = record.value();
          if (MODERATED_STATUS.equals(value)) {
            continue;
          }
          LOG.info("Got moderation issue for '{}': {}", key, value);
          onModeration.accept(key);
        }
      }
    } catch (final WakeupException e) {
      LOG.info("Finishing consumer loop due to WakeupException");
    } catch (final RuntimeException e) {
      LOG.error("Unexpected exception in consumer loop", e);
    }
  }

  public void linkCreated(final String key, final String value) {
    producerExecutor.submit(() -> producer.send(new ProducerRecord<>(CREATED_TOPIC, key, value)));
  }

  public void stop() {
    consumer.wakeup();
    RuntimeException e = null;
    for (final AutoCloseable closeable : new AutoCloseable[]{consumerExecutor, producerExecutor, consumer, producer}) {
      if (closeable == null) {
        continue;
      }
      try {
        closeable.close();
      } catch (final Exception ex) {
        if (e == null) {
          e = new RuntimeException(ex);
        } else {
          e.addSuppressed(ex);
        }
      }
    }
    if (e != null) {
      throw e;
    }
  }
}
