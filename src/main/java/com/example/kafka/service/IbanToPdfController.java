package com.example.kafka.service;

import com.example.kafka.model.IbanData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Controller, der eine PDF-Datei mit einer Liste von IBANs erstellt.
 * Diese Klasse stellt einen Endpunkt zur Verfügung, über den ein JSON-Array von IBANs empfangen wird,
 * das dann in einer PDF-Datei umgewandelt und als Download angeboten wird.
 */
@RestController
public class IbanToPdfController {

    /**
     * Erzeugt ein PDF aus einer Liste von IBANs, die als JSON-String übergeben wird.
     *
     * @param ibansJson Ein JSON-String, der eine Liste von IBAN-Objekten enthält.
     * @return Eine ResponseEntity mit dem generierten PDF als Byte-Array und den entsprechenden HTTP-Headern.
     * @throws IOException Wenn das Parsen des JSONs oder die PDF-Erstellung fehlschlägt.
     */
    @GetMapping("/generatePdf")
    public ResponseEntity<byte[]> generatePdf(@RequestParam("ibans") String ibansJson) throws IOException {
        // JSON von der URL holen und in eine Liste von IbanData umwandeln
        List<IbanData> ibans = parseIbanJson(ibansJson);

        // PDF generieren
        byte[] pdfBytes = generatePdf(ibans);

        // Response-Header für den PDF-Download setzen
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=\"iban_list.pdf\"");
        headers.add("Content-Type", "application/pdf");

        // PDF als Antwort zurückgeben
        return ResponseEntity.status(HttpStatus.OK)
                .headers(headers)
                .body(pdfBytes);
    }

    /**
     * Wandelt den JSON-String, der IBAN-Daten enthält, in eine Liste von IbanData-Objekten um.
     *
     * @param ibansJson Der JSON-String, der eine Liste von IBANs enthält.
     * @return Eine Liste von IbanData-Objekten.
     * @throws IOException Wenn das JSON nicht korrekt verarbeitet werden kann.
     */
    private List<IbanData> parseIbanJson(String ibansJson) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(ibansJson,
                objectMapper.getTypeFactory().constructCollectionType(List.class, IbanData.class));
    }

    /**
     * Generiert ein PDF aus einer Liste von IbanData-Objekten.
     *
     * @param ibans Eine Liste von IbanData-Objekten, die die zu druckenden IBANs enthält.
     * @return Ein Byte-Array, das das generierte PDF enthält.
     */
    private byte[] generatePdf(List<IbanData> ibans) {
        try {
            PDDocument document = new PDDocument();
            PDPage page = new PDPage();
            document.addPage(page);

            PDPageContentStream contentStream = new PDPageContentStream(document, page);
            contentStream.beginText();
            contentStream.setFont(PDType1Font.HELVETICA_BOLD, 12);
            contentStream.newLineAtOffset(50, 750);
            contentStream.showText("Liste der IBANs:");
            contentStream.newLineAtOffset(0, -20); // Zeilenabstand

            for (IbanData ibanData : ibans) {
                contentStream.showText(ibanData.getIban());
                contentStream.newLineAtOffset(0, -20); // Zeilenabstand
            }

            contentStream.endText();
            contentStream.close();

            // PDF in ByteArrayOutputStream speichern
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            document.save(outputStream);
            document.close();

            return outputStream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            return new byte[0];
        }
    }
}
