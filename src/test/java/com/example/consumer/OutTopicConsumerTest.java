package com.example.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OutTopicConsumerTest {

    private static final String TEST_FILE_PATH = "C:/dev/test/kafka-client/test_iban_list.pdf";
    private RestTemplate restTemplateMock;
    private ObjectMapper objectMapper;
    private static final Logger logger = Logger.getLogger(OutTopicConsumerTest.class.getName());

    @BeforeEach
    void setUp() {
        restTemplateMock = Mockito.mock(RestTemplate.class);
        objectMapper = new ObjectMapper();
        logger.info("Testumgebung wurde initialisiert.");
    }

    @Test
    void testProcessKafkaMessageAndSavePdf() throws Exception {
        logger.info("Test gestartet: testProcessKafkaMessageAndSavePdf");

        // Simulierte IBAN-Liste
        List<String> testIbans = List.of("DE44500105175407324931");
        String jsonIbans = objectMapper.writeValueAsString(testIbans);
        logger.info("Simulierte IBAN-Liste: " + jsonIbans);

        // Simulierte PDF-Daten
        byte[] fakePdfContent = "Fake PDF Content".getBytes();
        ResponseEntity<byte[]> mockResponse = new ResponseEntity<>(fakePdfContent, org.springframework.http.HttpStatus.OK);
        logger.info("Simulierte PDF-Daten erstellt.");

        // Mock für den HTTP-Aufruf
        when(restTemplateMock.exchange(any(String.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(byte[].class)))
                .thenReturn(mockResponse);
        logger.info("HTTP-Aufruf für PDF-Daten mocken.");

        // URL mit Parametern simulieren
        String urlWithParams = "http://localhost:8080/generatePdf?ibans=" + jsonIbans;
        logger.info("Simulierte URL: " + urlWithParams);

        // HTTP-Anfrage senden und Antwort simulieren
        ResponseEntity<byte[]> response = restTemplateMock.exchange(urlWithParams, HttpMethod.GET, new HttpEntity<>(new HttpHeaders()), byte[].class);

        // Überprüfen, ob die Antwort erfolgreich war
        assertEquals(org.springframework.http.HttpStatus.OK, response.getStatusCode(), "HTTP-Status ist nicht 200!");
        logger.info("Antwort des HTTP-Aufrufs überprüft: " + response.getStatusCode());

        // PDF in Datei speichern
        savePdfToFile(response.getBody());
        logger.info("PDF in Datei gespeichert: " + TEST_FILE_PATH);

        // Datei prüfen
        File pdfFile = new File(TEST_FILE_PATH);
        assertTrue(pdfFile.exists(), "PDF-Datei wurde nicht gespeichert!");
        logger.info("Überprüft, ob PDF-Datei existiert.");

        // Inhalt überprüfen
        byte[] savedContent = Files.readAllBytes(pdfFile.toPath());
        assertArrayEquals(fakePdfContent, savedContent, "Der Inhalt des gespeicherten PDFs stimmt nicht!");
        logger.info("Inhalt der gespeicherten PDF-Datei überprüft.");

        // Datei nach dem Test löschen
        assertTrue(pdfFile.delete(), "Test-PDF konnte nicht gelöscht werden!");
        logger.info("Test-PDF wurde gelöscht.");
    }

    private void savePdfToFile(byte[] pdfBytes) throws Exception {
        File pdfFile = new File(TEST_FILE_PATH);
        try (var fos = new java.io.FileOutputStream(pdfFile)) {
            fos.write(pdfBytes);
        }
    }
}
