# Kafka IBAN Checker

Dieses Projekt besteht aus zwei Komponenten: einem Kafka Producer und einem Kafka Consumer, die IBANs aus einer
JSON-Datei verarbeiten und in verschiedene Kafka-Topics senden.

## Funktionen

1. **IbanProducer**:
    - Liest eine JSON-Datei im Ordner `unchecked`.
    - Extrahiert IBANs und sendet sie an das Kafka-Topic `json-topic`.
    - Verschiebt verarbeitete Dateien in den Ordner `processed`.

2. **IbanConsumer**:
    - Liest IBANs vom Kafka-Topic `json-topic`.
    - Überprüft, ob die IBAN in der Blacklist oder Whitelist enthalten ist und sendet sie an die entsprechenden
      Kafka-Topics:
        - `json-verify-blacklisted` für IBANs, die nicht auf der Whitelist sind.
        - `json-verify-whitelisted` für IBANs, die mit "DE" beginnen und auf der Whitelist sind.
    - falls Ibans schon verarbeitet wurden, werden diese übersprungen und es werden keine weiteren Schritte mehr
      unternommen
3. **OutTopicConsumer**:
    - Liest IBANS vom Kafka-Topic `json-verify-blacklisted`
        - Erstellt falls nicht vorhanden den blacklisted Ordner im Projektverzeichnis
        - konsumiert aus den `out-topic` die blacklisted Ibans und übergibt diese dann an den Endpunkt /generatePDF als
          Parameter
        - speichert das generierte PDF

4. **IbanToPdfController**:
    - Ruft den Restendpunkt /generatePDF auf
        - speichert aus dem out-topic, welches die blacklisted Ibans enthält in eine Liste von IbanData Objekten
        - generiert das PDF

## Verzeichnisse

- `unchecked`: Enthält JSON-Dateien, die verarbeitet werden müssen. Dateinamen wird mit aktuellem Datum ergänzt
- `processed`: Enthält bereits verarbeitete JSON-Dateien.
- `blacklistedIbans`: Enthält die PDF mit aktuellem Datum der blacklisted IBans.

## Voraussetzungen

- Java 8 oder höher
- Docker Desktop
- Apache Kafka und Zookeeper laufen lokal auf `localhost:9092`
- Apache Kafka UI (nicht zwingend erforderlich) laufen lokal auf `localhost:8081`
- SpringBootApplication läuft lokal auf `localhost:8080`

## Wie man es benutzt

1. Starten Sie den Kafka-Server lokal.
    - Konfigurierte docker-compose.yaml Datei
    - Starten des Docker Servers. Befehl in dem Ordner, wo die docker-compose.yaml liegt (`docker-compose up -d`)

2. Legen Sie JSON-Dateien in den Ordner `unchecked` ab.
3. Starten Sie den Kafka Producer (`IbanProducer`), um die Dateien zu verarbeiten.
4. Starten Sie den Kafka Consumer (`IbanConsumer`), um IBANs zu überprüfen und in die entsprechenden Topics zu senden.
5. Starten Sie den OutTopic Consumer (`OutTopicConsumer`), speichert die blacklisted IBans und übergibt sie an den
   Endpunkt /generatePDF und hängt die IBans an die URL an
6. Starten Sie den MySpringBoot Application(`MySpringBootApplication`)

## Verbesserungen

- LOGGER Meldungen erweitern
- PDF einlesen, daraus die IBans in einer JSon Datei abspeicheren und diese dann verarbeiten
- Blacklist Check erweitern. Momentan filtert der Check nur ob eine IBan mit DE oder nicht DE beginnt. DE=whitelisted
  andere=blacklisted
- Main Methoden in eine Start Applikation auslagern

## Beispiel-JSON-Datei (`personen.json`)

```json
{
  "personen": [
    {
      "name": "Max Mustermann",
      "bankdaten": {
        "iban": "DE89370400440532013000"
      }
    },
    {
      "name": "Erika Musterfrau",
      "bankdaten": {
        "iban": "GB29XABC10161234567801"
      }
    }
  ]
}

