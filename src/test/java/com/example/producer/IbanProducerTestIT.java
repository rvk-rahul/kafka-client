package com.example.producer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class IbanProducerTestIT {

    private static final String TOPIC = "json-topic";
    private Consumer<String, String> consumer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @AfterEach
    void cleanUpProcessedFiles() {
        File processedDir = new File("processed");
        if (processedDir.exists()) {
            File[] files = processedDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.delete()) {
                        System.out.println("Gelöscht: " + file.getName());
                    } else {
                        System.err.println("Fehler beim Löschen der Datei: " + file.getName());
                    }
                }
            }
        }
    }

    @Test
    void testIbanProducerSendsCorrectData(@TempDir Path tempDir) throws IOException, InterruptedException {
        // JSON-Datei mit IBAN erstellen
        File tempFile = tempDir.resolve("test.json").toFile();
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8)) {
            writer.write("{\"iban\":\"DE44500105175407324931\"}");
        }

        // Datei in den "unchecked"-Ordner verschieben
        File uncheckedDir = new File("unchecked");
        if (!uncheckedDir.exists()) {
            uncheckedDir.mkdirs();
        }
        File testFile = new File(uncheckedDir, tempFile.getName());

        assertTrue(tempFile.renameTo(testFile), "Die Datei konnte nicht nach 'unchecked' verschoben werden");

        // Wartezeit, damit der Producer die Datei verarbeiten kann
        Thread.sleep(5000);

        // Nachrichten von Kafka konsumieren
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        assertFalse(records.isEmpty(), "Kafka hat keine Nachrichten empfangen");

        // Überprüfung des gesendeten IBANs
        records.forEach(record -> {
            try {
                List<String> ibans = objectMapper.readValue(record.value(), new TypeReference<>() {
                });
                assertEquals(1, ibans.size());
                assertEquals("DE44500105175407324931", ibans.get(0));
            } catch (IOException e) {
                fail("Fehler beim Parsen des JSON: " + e.getMessage());
            }
        });

        // Suche nach der Datei mit Präfix
        File processedDir = new File("processed");
        assertTrue(processedDir.exists(), "Verzeichnis 'processed' existiert nicht");

        boolean fileFound = false;
        for (int i = 0; i < 5; i++) {  // Wiederhole bis zu 5-mal mit Wartezeit
            File[] matchingFiles = processedDir.listFiles((dir, name) -> name.startsWith("test_") && name.endsWith(".json"));
            if (matchingFiles != null && matchingFiles.length > 0) {
                fileFound = true;
                System.out.println("Gefundene Datei: " + matchingFiles[0].getName());
                break;
            }
            Thread.sleep(1000);  // Warte 1 Sekunde und prüfe erneut
        }

        assertTrue(fileFound, "Die Datei wurde nicht korrekt in den 'processed'-Ordner verschoben.");
    }


}
