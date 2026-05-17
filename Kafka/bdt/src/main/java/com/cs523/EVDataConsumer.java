package com.cs523;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EVDataConsumer {

    private static final String GROUP_ID = "ev-consumer-group";
    private static final int IDLE_POLLS_BEFORE_EXIT = 3;

    public enum Mode {
        LOCATION,
        EVDATA
    }

    private final String topic;
    private final Mode mode;
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Pattern NON_ALNUM = Pattern.compile("[^a-z0-9]");

    // Simple in-memory analytics counters
    private final Map<String, Integer> makeCount = new HashMap<>();
    private final Map<String, Integer> evTypeCount = new HashMap<>();
    private final Map<String, Integer> countyCount = new HashMap<>();
    private final Map<String, Integer> cityCount = new HashMap<>();
    private int totalConsumed = 0;

    public EVDataConsumer() {
        this(EVDataProducer.TOPIC_EVDATA, Mode.EVDATA);
    }

    public EVDataConsumer(String topic, Mode mode) {
        this.topic = topic;
        this.mode = mode;
        this.consumer = new KafkaConsumer<>(getConsumerProperties());
    }

    public static EVDataConsumer createLocationConsumer() {
        return new EVDataConsumer(EVDataProducer.TOPIC_LOCATION, Mode.LOCATION);
    }

    public static EVDataConsumer createEVDataConsumer() {
        return new EVDataConsumer(EVDataProducer.TOPIC_EVDATA, Mode.EVDATA);
    }

    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return props;
    }

    /**
     * Subscribes to the EV topic and consumes records.
     *
     * @param maxMessages number of messages to consume before stopping; 0 means unlimited
     */
    public void consume(int maxMessages) {
        consumer.subscribe(Collections.singletonList(topic));
        running.set(true);
        int idlePolls = 0;

        System.out.println("Subscribed to topic: " + topic);
        System.out.println("Consumer group: " + GROUP_ID);
        System.out.println("Bootstrap servers: " + KafkaConfig.getBootstrapServers());
        if (maxMessages == 0) {
            System.out.println("Unlimited mode with auto-exit after " + IDLE_POLLS_BEFORE_EXIT + " idle polls.");
        } else {
            System.out.println("Bounded mode: will consume up to " + maxMessages
                    + " records and auto-exit after " + IDLE_POLLS_BEFORE_EXIT
                    + " idle polls when no more records are available.");
        }
        System.out.println();

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    // Do not count idle time while the consumer is still joining/rebalancing.
                    if (consumer.assignment().isEmpty()) {
                        continue;
                    }

                    idlePolls++;
                    if (idlePolls >= IDLE_POLLS_BEFORE_EXIT) {
                        if (maxMessages == 0) {
                            System.out.println("\nNo new records for " + IDLE_POLLS_BEFORE_EXIT + " seconds. Stopping consumer.");
                        } else {
                            System.out.println("\nNo more records available after " + IDLE_POLLS_BEFORE_EXIT
                                    + " idle polls. Stopping at " + totalConsumed + " record(s) (requested up to "
                                    + maxMessages + ").");
                        }
                        running.set(false);
                    }
                    continue;
                }

                idlePolls = 0;

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                    totalConsumed++;
                    commitProcessedOffset(record);

                    if (maxMessages > 0 && totalConsumed >= maxMessages) {
                        System.out.println("\nReached max messages limit: " + maxMessages);
                        running.set(false);
                        break;
                    }
                }

                if (!records.isEmpty()) {
                    printProgressSummary();
                }
            }
        } catch (WakeupException e) {
            // Expected on stop()
        } finally {
            printFinalSummary();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            Map<String, String> fields = objectMapper.readValue(
                    record.value(), new TypeReference<Map<String, String>>() {});

            if (mode == Mode.LOCATION) {
                String county = getField(fields, "county");
                String city = getField(fields, "city");
                countyCount.merge(county, 1, Integer::sum);
                cityCount.merge(city, 1, Integer::sum);
                printLocationRecord(record, fields);
            } else {
                String make = getField(fields, "make");
                String evType = getField(fields, "electric_vehicle_type", "ev_type");
                makeCount.merge(make, 1, Integer::sum);
                evTypeCount.merge(evType, 1, Integer::sum);
                printEVDataRecord(record, fields, make, evType);
            }

        } catch (Exception e) {
            System.err.println("Failed to parse record at offset " + record.offset() + ": " + e.getMessage());
        }
    }

    private void printLocationRecord(ConsumerRecord<String, String> record, Map<String, String> fields) {
        System.out.printf(
            "[LOCATION][partition=%d, offset=%d]%n" +
            "  VIN               : %s%n" +
            "  County            : %s%n" +
            "  City              : %s%n" +
            "  State             : %s%n" +
            "  Postal Code       : %s%n" +
            "  DOL Vehicle ID    : %s%n" +
            "  Vehicle Location  : %s%n" +
            "%n",
            record.partition(),
            record.offset(),
            getField(fields, "vin_(1-10)", "vin", "vin_1_10"),
            getField(fields, "county"),
            getField(fields, "city"),
            getField(fields, "state"),
            getField(fields, "postal_code", "postalcode"),
            getField(fields, "dol_vehicle_id", "dolvehicleid"),
            getField(fields, "vehicle_location", "vehiclelocation")
        );
    }

    private void printEVDataRecord(ConsumerRecord<String, String> record, Map<String, String> fields,
            String make, String evType) {
        System.out.printf(
            "[EVDATA][partition=%d, offset=%d]%n" +
            "  VIN               : %s%n" +
            "  Model Year        : %s%n" +
            "  Make              : %s%n" +
            "  Model             : %s%n" +
            "  EV Type           : %s%n" +
            "  CAFV Eligibility  : %s%n" +
            "  Electric Range    : %s miles%n" +
            "  Legislative Dist. : %s%n" +
            "  DOL Vehicle ID    : %s%n" +
            "  Electric Utility  : %s%n" +
            "  2020 Census Tract : %s%n" +
            "%n",
            record.partition(),
            record.offset(),
            getField(fields, "vin_(1-10)", "vin", "vin_1_10"),
            getField(fields, "model_year", "modelyear"),
            make,
            getField(fields, "model"),
            evType,
            getField(fields, "clean_alternative_fuel_vehicle_(cafv)_eligibility", "cafv_eligibility"),
            getField(fields, "electric_range", "electricrange"),
            getField(fields, "legislative_district", "legislativedistrict"),
            getField(fields, "dol_vehicle_id", "dolvehicleid"),
            getField(fields, "electric_utility", "electricutility"),
            getField(fields, "2020_census_tract", "census_tract_2020", "censustract2020")
        );
    }

    private String getField(Map<String, String> fields, String... candidates) {
        for (String candidate : candidates) {
            String direct = fields.get(candidate);
            if (direct != null && !direct.isBlank()) {
                return direct;
            }
        }

        for (Map.Entry<String, String> entry : fields.entrySet()) {
            String normalizedKey = normalizeKey(entry.getKey());
            for (String candidate : candidates) {
                if (normalizedKey.equals(normalizeKey(candidate))) {
                    String value = entry.getValue();
                    if (value != null && !value.isBlank()) {
                        return value;
                    }
                }
            }
        }

        return "N/A";
    }

    private String normalizeKey(String value) {
        String sanitized = value == null ? "" : value.toLowerCase().replace("\ufeff", "").trim();
        return NON_ALNUM.matcher(sanitized).replaceAll("");
    }

    private void commitProcessedOffset(ConsumerRecord<String, String> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata nextOffset = new OffsetAndMetadata(record.offset() + 1);
        consumer.commitSync(Collections.singletonMap(partition, nextOffset));
    }

    private void printProgressSummary() {
        System.out.println("--- Consumed so far: " + totalConsumed + " records ---");
    }

    private void printFinalSummary() {
        System.out.println("\n========== Consumer Summary ==========");
        System.out.println("Total records consumed: " + totalConsumed);

        if (mode == Mode.LOCATION) {
            System.out.println("\nTop Counties:");
            countyCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(e -> System.out.printf("  %-20s : %d%n", e.getKey(), e.getValue()));

            System.out.println("\nTop Cities:");
            cityCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(e -> System.out.printf("  %-30s : %d%n", e.getKey(), e.getValue()));
        } else {
            System.out.println("\nTop EV Makes:");
            makeCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(e -> System.out.printf("  %-20s : %d%n", e.getKey(), e.getValue()));

            System.out.println("\nEV Types:");
            evTypeCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .forEach(e -> System.out.printf("  %-50s : %d%n", e.getKey(), e.getValue()));
        }

        System.out.println("======================================");
    }

    /**
     * Signals the consume loop to stop gracefully.
     */
    public void stop() {
        running.set(false);
        consumer.wakeup();
    }

    /**
     * Closes the underlying Kafka consumer.
     */
    public void close() {
        consumer.close();
    }
}
