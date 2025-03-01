package com.example.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

/**
 * Der IbanConsumer empfängt IBAN-Daten von einem Kafka-Topic, verarbeitet sie und sendet sie je nach ihrer Herkunft
 * in zwei unterschiedliche Kafka-Topics (Blacklist oder Whitelist). IBANs aus der Blacklist werden an ein weiteres
 * Kafka-Topic zur weiteren Verarbeitung gesendet.
 */
public class IbanConsumer {

    private static final String TOPIC = "json-topic";
    private static final String BLACKLISTED_TOPIC = "json-verify-blacklisted";
    private static final String WHITELISTED_TOPIC = "json-verify-whitelisted";
    private static final String OUT_TOPIC = "out-topic";

    private static final Properties producerProps = new Properties();
    private static final Properties consumerProps = new Properties();

    static {
        // Kafka-Producer-Eigenschaften
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        // Kafka-Consumer-Eigenschaften
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
    private static final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

    /**
     * Der Einstiegspunkt der Anwendung, der den Kafka-Consumer startet und auf Nachrichten wartet.
     * Verarbeitet empfangene IBANs und entscheidet, ob sie auf die Blacklist oder Whitelist gesetzt werden.
     *
     * @param args Kommandozeilenargumente (werden hier nicht verwendet).
     */
    public static void main(String[] args) {
        consumer.subscribe(List.of(TOPIC));
        Set<String> processedIbans = new HashSet<>();
        Set<String> blacklistedIbans = new HashSet<>();
        ObjectMapper objectMapper = new ObjectMapper();

        while (true) {
            var records = consumer.poll(java.time.Duration.ofMillis(1000));
            records.forEach(record -> {
                try {
                    String message = record.value();
                    System.out.println("Empfangene Nachricht: " + message);
                    if (!message.startsWith("[") && !message.startsWith("{")) {
                        System.err.println("Fehler: Ungültige JSON-Nachricht empfangen -> " + message);
                        return;
                    }
                    List<String> ibans = objectMapper.readValue(message, List.class);
                    processIbans(producer, processedIbans, objectMapper, ibans, blacklistedIbans);
                    if (!blacklistedIbans.isEmpty()) {
                        sendBlacklistedIbansToOutTopic(producer, objectMapper, blacklistedIbans);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * Verarbeitet eine Liste von IBANs, überprüft, ob sie bereits verarbeitet wurden, und sendet sie an das
     * entsprechende Kafka-Topic (Blacklist oder Whitelist).
     *
     * @param producer         Der Kafka-Producer, der Nachrichten sendet.
     * @param processedIbans   Eine Menge bereits verarbeiteter IBANs.
     * @param objectMapper     Der Jackson ObjectMapper zur Umwandlung von Objekten in JSON.
     * @param ibans            Die Liste der zu verarbeitenden IBANs.
     * @param blacklistedIbans Eine Menge der IBANs, die auf der Blacklist landen.
     * @throws JsonProcessingException Wenn die Umwandlung in JSON fehlschlägt.
     */
    public static void processIbans(KafkaProducer<String, String> producer, Set<String> processedIbans,
                                    ObjectMapper objectMapper, List<String> ibans, Set<String> blacklistedIbans) throws JsonProcessingException {
        if (ibans != null) {
            for (String iban : ibans) {
                if (processedIbans.contains(iban)) {
                    System.out.println("IBAN " + iban + " wurde bereits verarbeitet.");
                    continue;
                }
                processedIbans.add(iban);
                String topicToSend = determineTopic(iban);
                String jsonMessage = objectMapper.writeValueAsString(iban);
                producer.send(new ProducerRecord<>(topicToSend, jsonMessage));
                if (topicToSend.equals(BLACKLISTED_TOPIC)) {
                    blacklistedIbans.add(iban);
                }
            }
        }
    }

    /**
     * Sendet die IBANs, die auf der Blacklist gelandet sind, an das OUT-Topic zur weiteren Verarbeitung.
     *
     * @param producer         Der Kafka-Producer, der Nachrichten sendet.
     * @param objectMapper     Der Jackson ObjectMapper zur Umwandlung von Objekten in JSON.
     * @param blacklistedIbans Eine Menge der IBANs, die auf der Blacklist sind.
     * @throws JsonProcessingException Wenn die Umwandlung in JSON fehlschlägt.
     */
    public static void sendBlacklistedIbansToOutTopic(KafkaProducer<String, String> producer,
                                                      ObjectMapper objectMapper, Set<String> blacklistedIbans) throws JsonProcessingException {
        String blacklistedJson = objectMapper.writeValueAsString(blacklistedIbans);
        producer.send(new ProducerRecord<>(OUT_TOPIC, blacklistedJson));
        blacklistedIbans.clear();
    }

    /**
     * Bestimmt das Kafka-Topic, in das eine IBAN gesendet wird, basierend auf ihrem Präfix.
     *
     * @param iban Die zu prüfende IBAN.
     * @return Das Topic, in das die IBAN gesendet wird (Whitelist oder Blacklist).
     */
    public static String determineTopic(String iban) {
        return iban.startsWith("DE") ? WHITELISTED_TOPIC : BLACKLISTED_TOPIC;
    }
}
