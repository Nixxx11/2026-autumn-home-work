package company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation.ModerationSystem.*;

public class ModerationServiceImpl implements ModerationService {
  private static final Logger LOG = LoggerFactory.getLogger(ModerationServiceImpl.class);

  private static final String GROUP_ID = "moderation-service";
  private static final String BANNED_WORDS_FILE = "/moderation/banned_words.txt";

  private final String bootstrapServers;
  private KafkaConsumer<String, String> consumer;
  private KafkaProducer<String, String> producer;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  public ModerationServiceImpl(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public void start() {
    final List<String> bannedWords;
    try {
      bannedWords = readBannedWords();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    consumer = createConsumer(bootstrapServers, GROUP_ID);
    consumer.subscribe(List.of(CREATED_TOPIC));
    producer = createProducer(bootstrapServers);
    executorService.submit(() -> runKafkaConsumer(bannedWords));
  }

  private static List<String> readBannedWords() throws IOException {
    try (InputStream input = ModerationServiceImpl.class.getResourceAsStream(BANNED_WORDS_FILE)) {
      if (input == null) {
        throw new IllegalStateException("Banned words file not found");
      }
      final BufferedReader reader = new BufferedReader(new InputStreamReader(input));
      return reader.lines().toList();
    }
  }

  private void runKafkaConsumer(final List<String> bannedWords) {
    try {
      while (true) {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (final ConsumerRecord<String, String> record : records) {
          final String key = record.key();
          final String value = record.value();

          ProducerRecord<String, String> result;
          try {
            final String status = processRecord(key, value, bannedWords);
            result = new ProducerRecord<>(MODERATED_TOPIC, key, status);
          } catch (final RuntimeException e) {
            LOG.error("Failed to process record with key '{}'", key, e);
            result = new ProducerRecord<>(DEAD_LETTER_TOPIC, key, value);
            result.headers().add("error", e.toString().getBytes(StandardCharsets.UTF_8));
          }
          producer.send(result);
        }
      }
    } catch (final WakeupException e) {
      LOG.info("Finishing consumer loop due to WakeupException");
    } catch (final RuntimeException e) {
      LOG.error("Unexpected exception in consumer loop", e);
    }
  }

  private static String processRecord(
      final String key,
      final String value,
      final List<String> bannedWords
  ) {
    final String decodedValue = URLDecoder.decode(value, StandardCharsets.UTF_8);
    for (final String bannedWord : bannedWords) {
      if (decodedValue.contains(bannedWord)) {
        final String message = String.format("Link %s contains banned word %s", value, bannedWord);
        LOG.info("Creating moderation issue for key '{}': {}", key, message);
        return message;
      }
    }
    LOG.debug("Successful moderation for key '{}'", key);
    return MODERATED_STATUS;
  }

  @Override
  public void stop() {
    consumer.wakeup();
    RuntimeException e = null;
    for (final AutoCloseable closeable : new AutoCloseable[]{executorService, consumer, producer}) {
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
