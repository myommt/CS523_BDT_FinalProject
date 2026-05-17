package com.cs523;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

public class EVDataProducer {
    public static final String TOPIC_LOCATION = KafkaConfig.getProperty("topic.location", "electric-vehicle-location");
    public static final String TOPIC_EVDATA = KafkaConfig.getProperty("topic.evdata", "electric-vehicle-evdata");
    public static final String DEFAULT_LOCATION_CSV = KafkaConfig.getProperty("csv.default.location", "Electric_Vehicle_Location_Data.csv");
    public static final String DEFAULT_EVDATA_CSV = KafkaConfig.getProperty("csv.default.evdata", "Electric_Vehicle_Data.csv");
    private static final String DOL_VEHICLE_ID_HEADER = "dol_vehicle_id";

    private KafkaProducer<String, String> producer;
    private String[] csvHeaders;

    public EVDataProducer() {
        this.producer = new KafkaProducer<>(getProducerProperties());
    }

    private Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        return props;
    }

    /**
     * Reads CSV file and sends each row to Kafka topic as JSON.
     *
     * @param csvFilePath path to the CSV file
     * @param maxMessages number of records to send; 0 means unlimited
     * @throws IOException if file reading fails
     * @throws CsvException if CSV parsing fails
     */
    public int produceFromCSV(String csvFilePath, int maxMessages) throws IOException, CsvException {
        return produceFromCSV(csvFilePath, TOPIC_EVDATA, maxMessages);
    }

    public int produceFromCSV(String csvFilePath, String topic, int maxMessages) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            List<String[]> allData = reader.readAll();

            if (allData.isEmpty()) {
                System.out.println("CSV file is empty");
                return 0;
            }

            // First row is headers
            this.csvHeaders = allData.get(0);
            // Strip UTF-8 BOM from first header if present (handles both U+FEFF and
            // the three Windows-1252-decoded BOM bytes \u00ef\u00bb\u00bf).
            if (csvHeaders.length > 0 && csvHeaders[0] != null) {
                csvHeaders[0] = csvHeaders[0].replaceAll("^[\ufeff\u00ef\u00bb\u00bf]+", "").trim();
            }
            System.out.println("Topic: " + topic);
            System.out.println("CSV Headers: " + String.join(", ", csvHeaders));

            // Process data rows (skip header)
            int recordCount = 0;
            for (int i = 1; i < allData.size(); i++) {
                if (maxMessages > 0 && recordCount >= maxMessages) {
                    break;
                }

                String[] row = allData.get(i);
                String jsonMessage = convertRowToJson(row);
                
                // Use DOL Vehicle ID as key when present, otherwise fallback to row index.
                String key = getMessageKey(row, i);
                
                // Create final copy for lambda
                final int recordNumber = i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending message: " + exception.getMessage());
                    } else {
                        System.out.println("Sent record " + recordNumber + " to partition " + metadata.partition() + 
                                         " with offset " + metadata.offset());
                    }
                });
                recordCount++;

                // Progress indicator
                if (recordCount % 100 == 0) {
                    System.out.println("Sent " + recordCount + " records...");
                }
            }

            producer.flush();
            System.out.println("Total records sent: " + recordCount);
            return recordCount;
        }
    }

    public void produceFromCSV(String csvFilePath) throws IOException, CsvException {
        produceFromCSV(csvFilePath, 0);
    }

    public int produceLocationData(String csvFilePath, int maxMessages) throws IOException, CsvException {
        return produceFromCSV(csvFilePath, TOPIC_LOCATION, maxMessages);
    }

    public int produceEVData(String csvFilePath, int maxMessages) throws IOException, CsvException {
        return produceFromCSV(csvFilePath, TOPIC_EVDATA, maxMessages);
    }

    public int produceBothDatasets(String locationCsvPath, String evDataCsvPath, int maxMessagesPerTopic)
            throws IOException, CsvException {
        int locationCount = produceLocationData(locationCsvPath, maxMessagesPerTopic);
        int evDataCount = produceEVData(evDataCsvPath, maxMessagesPerTopic);
        int total = locationCount + evDataCount;

        System.out.println("Published datasets summary:");
        System.out.println("  Location topic (" + TOPIC_LOCATION + "): " + locationCount + " records");
        System.out.println("  EVData topic (" + TOPIC_EVDATA + "): " + evDataCount + " records");
        System.out.println("  Total records across topics: " + total);
        return total;
    }

    /**
     * Converts a CSV row to JSON format using headers
     */
    private String convertRowToJson(String[] row) {
        StringBuilder json = new StringBuilder("{");
        
        for (int i = 0; i < csvHeaders.length && i < row.length; i++) {
            if (i > 0) {
                json.append(",");
            }
            String headerName = csvHeaders[i].replace(" ", "_").toLowerCase();
            String value = row[i].replace("\"", "\\\"");
            json.append("\"").append(headerName).append("\":\"").append(value).append("\"");
        }
        
        json.append("}");
        return json.toString();
    }

    private String getMessageKey(String[] row, int rowIndex) {
        for (int i = 0; i < csvHeaders.length && i < row.length; i++) {
            String headerName = csvHeaders[i].replace(" ", "_").toLowerCase();
            if (DOL_VEHICLE_ID_HEADER.equals(headerName)) {
                String value = row[i] == null ? "" : row[i].trim();
                if (!value.isEmpty()) {
                    return value;
                }
            }
        }

        return String.valueOf(rowIndex);
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    /*
    public static void main(String[] args) {
        String csvFilePath = args.length > 0 ? args[0] : DEFAULT_EVDATA_CSV;
        int maxMessages = 0;
        String topic = TOPIC_LEGACY;

        if (args.length > 1 && !isInteger(args[1])) {
            topic = args[1];
        }

        if (args.length > 1) {
            String maxMessagesArg = isInteger(args[1]) ? args[1] : (args.length > 2 ? args[2] : null);
            if (maxMessagesArg != null) {
                try {
                    maxMessages = Integer.parseInt(maxMessagesArg);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid max messages value: " + maxMessagesArg);
                    System.exit(1);
                }
            }
        }

        EVDataProducer producer = new EVDataProducer();
        try {
            producer.produceFromCSV(csvFilePath, topic, maxMessages);
        } catch (IOException | CsvException e) {
            System.err.println("Failed to produce data: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static boolean isInteger(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }

        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    */
}
