package company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation.ModerationSystem.CREATED_TOPIC;
import static company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation.ModerationSystem.MODERATED_STATUS;
import static company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation.ModerationSystem.MODERATED_TOPIC;
import static company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation.ModerationSystem.prepareConsumerProperties;
import static company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation.ModerationSystem.prepareProducerProperties;

public class ModerationServiceImpl implements ModerationService {
  private static final String GROUP_ID = "moderation-service";
  private static final String BANNED_WORDS_FILE = "/moderation/banned_words.txt";

  private final String bootstrapServers;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  public ModerationServiceImpl(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public void start() {
    final List<String> bannedWords;
    try {
      bannedWords = readBannedWords();
    } catch (final IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
    final Properties consumerProperties = prepareConsumerProperties(bootstrapServers, GROUP_ID);
    final Properties producerProperties = prepareProducerProperties(bootstrapServers);
    executorService.submit(() -> runKafkaConsumer(bannedWords, consumerProperties, producerProperties));
  }

  private static List<String> readBannedWords() throws IOException, URISyntaxException {
    final URL url = ModerationServiceImpl.class.getResource(BANNED_WORDS_FILE);
    if (url == null) {
      throw new IllegalStateException("Banned words file not found");
    }
    final Path path = Path.of(url.toURI());
    return Files.readString(path).lines().toList();
  }

  private static void runKafkaConsumer(
      final List<String> bannedWords,
      final Properties consumerProperties,
      final Properties producerProperties
  ) {
    try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
      try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
        consumer.subscribe(List.of(CREATED_TOPIC));
        while (true) {
          final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
          for (final ConsumerRecord<String, String> record : records) {
            final ProducerRecord<String, String> result = processRecord(record, bannedWords);
            producer.send(result);
          }
        }
      }
    }
  }

  private static ProducerRecord<String, String> processRecord(
      final ConsumerRecord<String, String> record,
      final List<String> bannedWords
  ) {
    final String key = record.key();
    final String value = record.value();
    for (final String bannedWord : bannedWords) {
      if (value.contains(bannedWord)) {
        final String message = String.format("Link %s contains banned word %s", key, bannedWord);
        return new ProducerRecord<>(MODERATED_TOPIC, key, message);
      }
    }
    return new ProducerRecord<>(MODERATED_TOPIC, key, MODERATED_STATUS);
  }

  @Override
  public void stop() {
    executorService.shutdownNow();
  }
}
