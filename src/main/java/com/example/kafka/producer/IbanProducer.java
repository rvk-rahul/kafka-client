package com.example.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ein Kafka Producer, der JSON-Dateien überwacht, die IBAN-Daten enthalten,
 * diese IBANs extrahiert und an ein Kafka-Topic sendet. Die Dateien werden
 * nach der Verarbeitung in ein "processed"-Verzeichnis verschoben.
 */
public class IbanProducer {
    private static final String TOPIC = "json-topic"; // Kafka-Topic, an das die IBANs gesendet werden
    private static final String UNCHECKED_DIR = "unchecked"; // Verzeichnis, in dem die zu verarbeitenden JSON-Dateien liegen
    private static final String PROCESSED_DIR = "processed"; // Verzeichnis, in das die verarbeiteten Dateien verschoben werden
    private static final ObjectMapper objectMapper = new ObjectMapper(); // JSON-Mapper zum Umwandeln der IBAN-Liste in JSON
    private static final KafkaProducer<String, String> producer; // KafkaProducer zum Senden der IBAN-Daten

    static {
        createDirectories();
        // Initialisierung der KafkaProducer-Eigenschaften
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
    }

    /**
     * Der Einstiegspunkt der Anwendung. Diese Methode überwacht das Verzeichnis "unchecked" auf neue JSON-Dateien,
     * verarbeitet sie und sendet die extrahierten IBAN-Daten an ein Kafka-Topic.
     *
     * @param args Die Kommandozeilenargumente (werden nicht verwendet).
     * @throws Exception Wenn beim Überwachen oder Verarbeiten der Dateien ein Fehler auftritt.
     */
    public static void main(String[] args) throws Exception {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Path uncheckedPath = Paths.get(UNCHECKED_DIR);
        uncheckedPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        System.out.println("Warte auf neue JSON-Dateien im Ordner 'unchecked'...");

        while (true) {
            WatchKey key = watchService.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent<Path> watchEvent = (WatchEvent<Path>) event;
                Path filename = watchEvent.context();
                File jsonFile = new File(UNCHECKED_DIR, filename.toString());

                if (jsonFile.getName().endsWith(".json")) {
                    waitForFileAvailability(jsonFile);
                    processJsonFile(jsonFile);
                }
            }
            key.reset();
        }
    }

    /**
     * Erstellt die erforderlichen Verzeichnisse, falls diese noch nicht existieren.
     */
    private static void createDirectories() {
        new File(UNCHECKED_DIR).mkdirs();
        new File(PROCESSED_DIR).mkdirs();
    }

    /**
     * Wartet, bis eine Datei für die Verarbeitung verfügbar ist.
     * Dies bedeutet, dass die Datei nicht mehr von einem anderen Prozess gesperrt ist.
     *
     * @param file Die Datei, die auf Verfügbarkeit geprüft werden soll.
     * @throws InterruptedException Wenn der Warteschlaf unterbrochen wird.
     */
    private static void waitForFileAvailability(File file) throws InterruptedException {
        int attempts = 5;
        while (attempts-- > 0) {
            if (isFileReady(file)) {
                return;
            }
            Thread.sleep(500);
        }
        System.err.println("Warnung: Datei konnte nicht rechtzeitig entsperrt werden: " + file.getName());
    }

    /**
     * Überprüft, ob eine Datei für die Verarbeitung verfügbar ist.
     *
     * @param file Die zu überprüfende Datei.
     * @return True, wenn die Datei verfügbar ist, andernfalls false.
     */
    private static boolean isFileReady(File file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Verarbeitet eine JSON-Datei, extrahiert die IBANs und sendet diese an Kafka.
     *
     * @param jsonFile Die zu verarbeitende JSON-Datei.
     */
    public static void processJsonFile(File jsonFile) {
        try {
            // Lese den Inhalt der JSON-Datei
            String content = Files.readString(jsonFile.toPath());
            List<String> ibanList = extractIbans(content);

            // Wenn IBANs extrahiert wurden, diese an Kafka senden
            if (!ibanList.isEmpty()) {
                String ibanJson = objectMapper.writeValueAsString(ibanList);
                sendToKafka(ibanJson);
            } else {
                System.err.println("Keine IBANs gefunden: " + jsonFile.getName());
            }

            // Die verarbeitete Datei in das 'processed'-Verzeichnis verschieben
            moveFileToProcessed(jsonFile);
        } catch (IOException e) {
            System.err.println("Fehler beim Verarbeiten der Datei: " + jsonFile.getName());
            e.printStackTrace();
        }
    }

    /**
     * Sendet die extrahierten IBANs als JSON an das Kafka-Topic.
     *
     * @param ibanJson Der JSON-String, der die IBANs enthält.
     */
    private static void sendToKafka(String ibanJson) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, ibanJson);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.println("IBANs gesendet: " + ibanJson);
            }
        });
        producer.flush();
    }

    /**
     * Extrahiert IBANs aus einem gegebenen Text.
     * IBANs müssen dem Standardformat entsprechen (z. B. DE44 1234 1234 1234 1234 00).
     *
     * @param content Der Text, aus dem IBANs extrahiert werden.
     * @return Eine Liste der extrahierten IBANs.
     */
    public static List<String> extractIbans(String content) {
        List<String> ibanList = new ArrayList<>();
        Pattern ibanPattern = Pattern.compile("[A-Z]{2}\\d{2}[A-Z0-9]{18}");
        Matcher matcher = ibanPattern.matcher(content);

        // Alle übereinstimmenden IBANs finden und zur Liste hinzufügen
        while (matcher.find()) {
            ibanList.add(matcher.group());
        }
        return ibanList;
    }

    /**
     * Verschiebt die verarbeitete Datei in das 'processed'-Verzeichnis.
     * Der Dateiname wird mit einem Zeitstempel versehen, um die Datei eindeutig zu benennen.
     *
     * @param file Die verarbeitete Datei.
     */
    private static void moveFileToProcessed(File file) {
        File processedDir = new File(PROCESSED_DIR);
        if (!processedDir.exists()) {
            processedDir.mkdirs();
        }

        // Erstelle einen neuen Dateinamen mit Zeitstempel
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date());
        String fileName = file.getName();
        String newFileName = fileName.replaceAll("_\\d{8}_\\d{4}", "")
                .replaceAll("\\.json", "")
                + "_" + timestamp + ".json";
        File processedFile = new File(processedDir, newFileName);

        // Versuche, die Datei zu verschieben
        if (file.renameTo(processedFile)) {
            System.out.println("Datei verschoben: " + processedFile.getName());
        } else {
            System.err.println("Fehler: Datei konnte nicht verschoben werden!");
        }
    }
}
