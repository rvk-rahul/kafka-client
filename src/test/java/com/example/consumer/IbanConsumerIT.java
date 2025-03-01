//package com.example;
//
//
//import com.example.kafka.config.KafkaConfig;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//import org.testcontainers.containers.KafkaContainer;
//import org.testcontainers.utility.DockerImageName;
//
//import java.time.Duration;
//import java.util.List;
//import java.util.Properties;
//import java.util.UUID;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class IbanConsumerIT {
//
//    private static KafkaContainer kafkaContainer;
//    private static String bootstrapServers;
//
//    @BeforeAll
//    static void setup() {
//        System.setProperty("DOCKER_HOST", "unix:///run/containerd/containerd.sock");
//        System.setProperty("org.testcontainers.logger", "slf4j");
//        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
//        kafkaContainer.start();
//        bootstrapServers = kafkaContainer.getBootstrapServers();
//    }
//
//    @AfterAll
//    static void tearDown() {
//        kafkaContainer.stop();
//    }
//
//    @Test
//    void printLine() throws Exception {
//        System.out.println("T");
//
//    }
//
//    @Test
//    void testKafkaProcessing() throws Exception {
//        // Kafka Producer konfigurieren
//        Properties producerProps = new Properties();
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
////            producer.send(new ProducerRecord<>("json-verify-whitelisted", "init")).get();
////            producer.send(new ProducerRecord<>("json-verify-blacklisted", "init")).get();
//            producer.send(new ProducerRecord<>("json-verify-whitelisted", "\"DE89370400440532013000\"")).get();
//            producer.send(new ProducerRecord<>("json-verify-blacklisted", "\"FR7630006000011234567890189\"")).get();
//
//            // Test-IBANs als JSON-Array senden
////            String testMessage = "[\"DE89370400440532013000\", \"FR7630006000011234567890189\"]";
////            producer.send(new ProducerRecord<>("json-topic", testMessage)).get();  // Sicherstellen, dass Kafka die Nachricht erhalten hat
//        }
//        Thread.sleep(5000);
//        // Kafka Consumer konfigurieren
//        Properties consumerProps = new Properties();
//        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
//        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
//            // Konsumieren der Nachrichten aus beiden Topics
//            consumer.subscribe(List.of("json-verify-whitelisted", "json-verify-blacklisted"));
//
//            // Längeren Poll-Timeout setzen
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
//
//            // Prüfen, ob die Nachrichten korrekt verarbeitet wurden
//            assertEquals(2, records.count(), "Es sollten zwei Nachrichten verarbeitet worden sein.");
//
//            records.forEach(record -> {
//                System.out.println("Record: " + record.topic() + " - " + record.value());
//                if (record.topic().equals("json-verify-whitelisted")) {
//                    assertEquals("\"DE89370400440532013000\"", record.value(), "Die deutsche IBAN sollte whitelisted sein.");
//                    System.out.println(record.value() + "++++++++++++++++++");
//                } else if (record.topic().equals("json-verify-blacklisted")) {
//                    System.out.println(record.value() + "++++++++++++++++++");
//                    assertEquals("\"FR7630006000011234567890189\"", record.value(), "Die französische IBAN sollte blacklisted sein.");
//                }
//            });
//        }
//    }
//}

package com.example.consumer;

import com.example.kafka.consumer.IbanConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;

import static com.example.kafka.consumer.IbanConsumer.sendBlacklistedIbansToOutTopic;
import static org.junit.jupiter.api.Assertions.*;

class IbanConsumerIT {

    private static KafkaContainer kafkaContainer;
    private static String bootstrapServers;

    @BeforeAll
    static void setup() {
        System.setProperty("DOCKER_HOST", "unix:///run/containerd/containerd.sock");
        System.setProperty("org.testcontainers.logger", "slf4j");
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
        kafkaContainer.start();
        bootstrapServers = kafkaContainer.getBootstrapServers();
    }

    @AfterAll
    static void tearDown() {
        kafkaContainer.stop();
    }

    @Test
    @Order(1)
    void testKafkaProcessing() throws Exception {

        // Kafka Producer konfigurieren
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Nachricht an json-topic senden
            producer.send(new ProducerRecord<>("json-verify-whitelisted", "\"DE89370400440532013000\"")).get();
            producer.send(new ProducerRecord<>("json-verify-blacklisted", "\"FR7630006000011234567890189\"")).get();

            // Vergewissere dich, dass eine Nachricht an "out-topic" gesendet wird
            Set<String> blacklistedIbans = new HashSet<>();
            blacklistedIbans.add("FR7630006000011234567890189");
            sendBlacklistedIbansToOutTopic(producer, new ObjectMapper(), blacklistedIbans);
        }

        // Wartezeit erhöhen, damit der Consumer Nachrichten abholen kann
        Thread.sleep(10000); // Erhöhe die Wartezeit

        // Kafka Consumer konfigurieren
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of("json-verify-whitelisted", "json-verify-blacklisted", "out-topic"));

            // Längeren Poll-Timeout setzen
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));

            // Debugging-Ausgabe
            System.out.println("Anzahl der Nachrichten verarbeitet: " + records.count());

            // Prüfen, ob die Nachrichten korrekt verarbeitet wurden
            assertEquals(3, records.count(), "Es sollten drei Nachrichten verarbeitet worden sein.");

            records.forEach(record -> {
                System.out.println("Topic: " + record.topic() + " Value: " + record.value());
                if (record.topic().equals("json-verify-whitelisted")) {
                    assertEquals("\"DE89370400440532013000\"", record.value(), "Die deutsche IBAN sollte whitelisted sein.");
                } else if (record.topic().equals("json-verify-blacklisted")) {
                    assertEquals("\"FR7630006000011234567890189\"", record.value(), "Die französische IBAN sollte blacklisted sein.");
                } else if (record.topic().equals("out-topic")) {
                    System.out.println("Blacklisted IBANs in OUT-TOPIC: " + record.value());
                    assertTrue(record.value().contains("\"FR7630006000011234567890189\""), "Die Blacklist-IBAN sollte im OUT-TOPIC enthalten sein.");
                }
            });

        }
    }

    @Test
    @Order(2)
    void testSendBlacklistedIbansToOutTopic() throws Exception {
        // Set up Kafka Producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        ObjectMapper objectMapper = new ObjectMapper();

        // Blacklist-IBANs
        Set<String> blacklistedIbans = new HashSet<>();
        blacklistedIbans.add("FR7630006000011234567890183");

        // Call method to send blacklisted IBANs to OUT_TOPIC
//        IbanConsumer.sendBlacklistedIbansToOutTopic(producer, objectMapper, blacklistedIbans);

        // Wait for the message to be processed
        Thread.sleep(5000);

        // Create a Kafka Consumer to check if the message was sent to OUT_TOPIC
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of("out-topic"));

        // Poll the consumer to get the message
        var records = consumer.poll(java.time.Duration.ofMillis(5000));

        // Check if the message contains the blacklisted IBANs
        records.forEach(record -> {
            assertEquals("[\"FR7630006000011234567890183\"]", record.value(), "Die Blacklist-IBAN sollte korrekt gesendet worden sein.");
        });
        consumer.seekToBeginning(consumer.assignment());
        consumer.close();

    }

    @Test
    @Order(3)
    void testDetermineTopic() {
        // Test IBANs
        String validIbanWhitelist = "DE89370400440532013000"; // Whitelisted IBAN (DE)
        String validIbanBlacklist = "FR7630006000011234567890189"; // Blacklisted IBAN (FR)

        // Test determineTopic method
        String topicForWhitelist = IbanConsumer.determineTopic(validIbanWhitelist);
        String topicForBlacklist = IbanConsumer.determineTopic(validIbanBlacklist);

        // Assertions
        assertEquals("json-verify-whitelisted", topicForWhitelist, "Die IBAN sollte in das Whitelisted-Topic gehen.");
        assertEquals("json-verify-blacklisted", topicForBlacklist, "Die IBAN sollte in das Blacklisted-Topic gehen.");
    }

}

