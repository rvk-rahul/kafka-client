package com.example.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ein Consumer, der Nachrichten aus einem Kafka-Topic empfängt, eine Liste von IBANs verarbeitet und
 * auf Grundlage dieser Daten ein PDF erstellt und speichert.
 * Der Consumer ruft einen externen PDF-Generierungsdienst auf und speichert die generierten PDF-Dateien
 * in einem lokalen Verzeichnis.
 */
public class OutTopicConsumer {

    private static final String OUT_TOPIC = "out-topic"; // Der Kafka-Topic, aus dem Nachrichten konsumiert werden
    private static final String PDF_GENERATE_URL = "http://localhost:8080/generatePdf"; // URL des PDF-Generierungsdienstes
    private static final String FILE_PATH_TEMPLATE = "blacklist_iban_%s_%d.pdf"; // Dateinamen-Template für die gespeicherten PDFs
    private static final Properties consumerProps = new Properties(); // Kafka Consumer-Eigenschaften
    private static final KafkaConsumer<String, String> consumer; // KafkaConsumer, um Nachrichten zu konsumieren
    private static final ObjectMapper objectMapper = new ObjectMapper(); // JSON-Verarbeitungs-Tool
    private static final RestTemplate restTemplate = new RestTemplate(); // HTTP-Client, um Anfragen an den PDF-Service zu senden
    private static final AtomicInteger fileCounter = new AtomicInteger(1); // Zähler für die PDF-Dateinamen

    static {
        // Kafka Consumer-Eigenschaften initialisieren
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Kafka Consumer initialisieren und für den Topic abonnieren
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(OUT_TOPIC));
    }

    /**
     * Der Einstiegspunkt der Anwendung, der kontinuierlich Nachrichten aus dem Kafka-Topic liest,
     * verarbeitet und PDFs erstellt.
     *
     * Die Nachrichten werden als JSON mit einer Liste von IBANs erwartet. Wenn IBANs empfangen werden,
     * wird eine Anfrage an den PDF-Generierungsdienst gesendet, der ein PDF mit diesen IBANs erstellt und
     * das PDF wird im lokalen Verzeichnis gespeichert.
     */
    public static void main(String[] args) {
        String projectDir = System.getProperty("user.dir");
        File blacklistDir = new File(projectDir, "blacklistedibans");

        // Verzeichnis für die gespeicherten PDFs erstellen, wenn es nicht existiert
        if (!blacklistDir.exists()) {
            blacklistDir.mkdirs();
            System.out.println("Ordner 'blacklistedibans' wurde erstellt.");
        }

        // Endlosschleife, um kontinuierlich Nachrichten aus Kafka zu konsumieren
        while (true) {
            var records = consumer.poll(java.time.Duration.ofMillis(1000));

            // Jede empfangene Nachricht verarbeiten
            records.forEach(record -> {
                try {
                    String message = record.value();
                    System.out.println("Empfangene Nachricht: " + message);

                    // Die IBANs aus der Nachricht extrahieren
                    List<String> blacklistedIbans = objectMapper.readValue(message, List.class);
                    if (!blacklistedIbans.isEmpty()) {
                        // Zeitstempel für den Dateinamen und Zähler für die Dateien
                        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date());
                        int fileIndex = fileCounter.getAndIncrement();
                        String filePath = new File(blacklistDir, String.format(FILE_PATH_TEMPLATE, timestamp, fileIndex)).getAbsolutePath();

                        // Query-Parameter für die PDF-Generierung
                        String queryParams = objectMapper.writeValueAsString(blacklistedIbans);
                        String urlWithParams = PDF_GENERATE_URL + "?ibans=" + queryParams;

                        HttpHeaders headers = new HttpHeaders();
                        HttpEntity<String> entity = new HttpEntity<>(headers);

                        // Anfrage an den PDF-Generierungsdienst senden
                        ResponseEntity<byte[]> response = restTemplate.exchange(urlWithParams, HttpMethod.GET, entity, byte[].class);

                        // PDF speichern, wenn die Generierung erfolgreich war
                        if (response.getStatusCode().is2xxSuccessful()) {
                            savePdfToFile(response.getBody(), filePath);
                            System.out.println("PDF erfolgreich generiert und gespeichert: " + filePath);
                        } else {
                            System.out.println("Fehler bei der PDF-Generierung: " + response.getStatusCode());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * Speichert die empfangenen PDF-Daten als Datei im angegebenen Pfad.
     *
     * @param pdfBytes Die PDF-Daten als Byte-Array.
     * @param filePath Der vollständige Pfad, unter dem die PDF-Datei gespeichert werden soll.
     * @throws IOException Wenn beim Speichern der Datei ein Fehler auftritt.
     */
    private static void savePdfToFile(byte[] pdfBytes, String filePath) throws IOException {
        File pdfFile = new File(filePath);
        try (FileOutputStream fos = new FileOutputStream(pdfFile)) {
            fos.write(pdfBytes);
        }
    }
}
