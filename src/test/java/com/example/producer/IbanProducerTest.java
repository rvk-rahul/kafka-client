package com.example.producer;

import com.example.kafka.producer.IbanProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockitoAnnotations;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class IbanProducerTest {


    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

    }

    @Test
    void testExtractIbans() {
        String jsonContent = "{\\\"iban\\\":\\\"DE4450010517540732493100\\\"}";
        List<String> ibans = IbanProducer.extractIbans(jsonContent);
        for (String iban : ibans) {
            System.out.println(iban);
        }
        assertEquals(1, ibans.size());
        assertEquals("DE44500105175407324931", ibans.get(0));
    }

    @Test
    void testProcessJsonFile(@TempDir Path tempDir) throws IOException, InterruptedException {
        // JSON-Datei mit IBAN erstellen
        File tempFile = tempDir.resolve("test.json").toFile();
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8)) {
            writer.write("{\"iban\":\"DE44500105175407324931\"}");
        }

        // Producer simulieren
        IbanProducer.processJsonFile(tempFile);

        // Wartezeit, damit die Datei verschoben werden kann
        Thread.sleep(2000);

        // Verzeichnis "processed" prüfen
        File processedDir = new File("processed");
        assertTrue(processedDir.exists(), "Das Verzeichnis 'processed' existiert nicht");

        // Datei mit Präfix "test_" und Suffix ".json" suchen
        File[] matchingFiles = processedDir.listFiles((dir, name) -> name.startsWith("test_") && name.endsWith(".json"));

        assertNotNull(matchingFiles, "Fehler beim Lesen des 'processed'-Verzeichnisses");
        assertTrue(matchingFiles.length > 0, "Die Datei wurde nicht korrekt verschoben");

        System.out.println("Gefundene Datei: " + matchingFiles[0].getName());
    }
}

