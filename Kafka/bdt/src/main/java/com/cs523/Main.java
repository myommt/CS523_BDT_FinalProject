package com.cs523;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public class Main {
    private static final List<String> TOPICS = List.of(
            EVDataProducer.TOPIC_LOCATION,
            EVDataProducer.TOPIC_EVDATA);
    private static final int DEFAULT_PARTITIONS = 3;
    private static final short DEFAULT_REPLICATION_FACTOR = 1;

    public static void main(String[] args) {
        System.out.println("=== Kafka Electric Vehicle Data Pipeline ===\n");
        System.out.println("Before running this application, ensure:");
        System.out.println("1. Kafka broker is running on localhost:9092");
        System.out.println("2. The CSV file is in the current directory\n");

        if (args.length == 0) {
            printUsage();
            interactiveMode();
        } else {
            commandLineMode(args);
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java Main produce <csv-file-path> [max-messages]");
        System.out.println("  java Main produce <csv-file-path> <topic-name> [max-messages]");
        System.out.println("  java Main produce-both [location-csv-path] [evdata-csv-path] [max-messages-per-topic]");
        System.out.println("  java Main consume [max-messages]");
        System.out.println("  java Main reset");
        System.out.println("  java Main (interactive mode)\n");
    }

    private static void interactiveMode() {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            System.out.println("\n--- Main Menu ---");
            System.out.println("1. Run Producer (single CSV to one topic)");
            System.out.println("2. Run Producer (both CSVs to two topics)");
            System.out.println("3. Run Consumer (read from Kafka)");
            System.out.println("4. Delete Topics Before Rerun");
            System.out.println("5. Exit");
            System.out.print("Select option (1-5): ");

            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1" -> {
                    System.out.print("Enter CSV file path (default: Electric_Vehicle_Data.csv): ");
                    String csvPath = scanner.nextLine().trim();
                    if (csvPath.isEmpty()) {
                        csvPath = EVDataProducer.DEFAULT_EVDATA_CSV;
                    }
                    System.out.print("Enter topic name (default: " + EVDataProducer.TOPIC_EVDATA + "): ");
                    String topic = scanner.nextLine().trim();
                    if (topic.isEmpty()) {
                        topic = EVDataProducer.TOPIC_EVDATA;
                    }
                    System.out.print("Enter max messages to produce (0 for all): ");
                    String maxInput = scanner.nextLine().trim();
                    int maxMessages = 0;
                    try {
                        maxMessages = maxInput.isEmpty() ? 0 : Integer.parseInt(maxInput);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number, using all records");
                    }
                    runProducer(csvPath, topic, maxMessages);
                }
                case "2" -> {
                    System.out.print("Enter location CSV path (default: " + EVDataProducer.DEFAULT_LOCATION_CSV + "): ");
                    String locationCsvPath = scanner.nextLine().trim();
                    if (locationCsvPath.isEmpty()) {
                        locationCsvPath = EVDataProducer.DEFAULT_LOCATION_CSV;
                    }
                    System.out.print("Enter EVData CSV path (default: " + EVDataProducer.DEFAULT_EVDATA_CSV + "): ");
                    String evDataCsvPath = scanner.nextLine().trim();
                    if (evDataCsvPath.isEmpty()) {
                        evDataCsvPath = EVDataProducer.DEFAULT_EVDATA_CSV;
                    }
                    System.out.print("Enter max messages per topic (0 for all): ");
                    String maxInput = scanner.nextLine().trim();
                    int maxMessagesPerTopic = 0;
                    try {
                        maxMessagesPerTopic = maxInput.isEmpty() ? 0 : Integer.parseInt(maxInput);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number, using all records");
                    }
                    runProducerBoth(locationCsvPath, evDataCsvPath, maxMessagesPerTopic);
                }
                case "3" -> {
                    System.out.print("Enter max messages to consume (0 for unlimited): ");
                    String maxInput = scanner.nextLine().trim();
                    int maxMessages = 0;
                    try {
                        maxMessages = maxInput.isEmpty() ? 0 : Integer.parseInt(maxInput);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number, using unlimited");
                    }
                    runConsumer(maxMessages);
                }
                case "4" -> {
                    resetTopicBeforeRerun();
                }
                case "5" -> {
                    System.out.println("Exiting...");
                    running = false;
                }
                default -> System.out.println("Invalid option. Please select 1-5.");
            }
        }

        scanner.close();
    }

    private static void commandLineMode(String[] args) {
        String command = args[0].toLowerCase();

        switch (command) {
            case "produce" -> {
                if (args.length < 2) {
                    System.err.println("Error: CSV file path required");
                    System.err.println("Usage: java Main produce <csv-file-path> [max-messages]");
                    System.err.println("   or: java Main produce <csv-file-path> <topic-name> [max-messages]");
                    System.exit(1);
                }

                String topic = EVDataProducer.TOPIC_EVDATA;
                int maxMessages = 0;

                if (args.length == 3) {
                    try {
                        maxMessages = Integer.parseInt(args[2]);
                    } catch (NumberFormatException e) {
                        topic = args[2];
                    }
                } else if (args.length > 3) {
                    topic = args[2];
                    try {
                        maxMessages = Integer.parseInt(args[3]);
                    } catch (NumberFormatException e) {
                        System.err.println("Error: Invalid max messages value");
                        System.exit(1);
                    }
                }
                runProducer(args[1], topic, maxMessages);
            }
            case "produce-both" -> {
                String locationCsvPath = args.length > 1 ? args[1] : EVDataProducer.DEFAULT_LOCATION_CSV;
                String evDataCsvPath = args.length > 2 ? args[2] : EVDataProducer.DEFAULT_EVDATA_CSV;
                int maxMessagesPerTopic = 0;

                if (args.length > 3) {
                    try {
                        maxMessagesPerTopic = Integer.parseInt(args[3]);
                    } catch (NumberFormatException e) {
                        System.err.println("Error: Invalid max messages value");
                        System.exit(1);
                    }
                }

                runProducerBoth(locationCsvPath, evDataCsvPath, maxMessagesPerTopic);
            }
            case "consume" -> {
                int maxMessages = 0;
                if (args.length > 1) {
                    try {
                        maxMessages = Integer.parseInt(args[1]);
                    } catch (NumberFormatException e) {
                        System.err.println("Error: Invalid max messages value");
                        System.exit(1);
                    }
                }
                runConsumer(maxMessages);
            }
            case "reset" -> resetTopicBeforeRerun();
            default -> {
                System.err.println("Unknown command: " + command);
                printUsage();
                System.exit(1);
            }
        }
    }

    private static void runProducer(String csvFilePath, String topic, int maxMessages) {
        System.out.println("\n--- Starting Producer ---");
        System.out.println("CSV File: " + csvFilePath);
        System.out.println("Topic: " + topic);
        System.out.println("Max Messages: " + (maxMessages == 0 ? "All" : maxMessages));

        EVDataProducer producer = new EVDataProducer();
        try {
            long startTime = System.currentTimeMillis();
            producer.produceFromCSV(csvFilePath, topic, maxMessages);
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Producer completed in " + duration + "ms");
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void runProducerBoth(String locationCsvPath, String evDataCsvPath, int maxMessagesPerTopic) {
        System.out.println("\n--- Starting Producer (Both Datasets) ---");
        System.out.println("Location CSV: " + locationCsvPath + " -> " + EVDataProducer.TOPIC_LOCATION);
        System.out.println("EVData CSV: " + evDataCsvPath + " -> " + EVDataProducer.TOPIC_EVDATA);
        System.out.println("Max Messages Per Topic: " + (maxMessagesPerTopic == 0 ? "All" : maxMessagesPerTopic));

        EVDataProducer producer = new EVDataProducer();
        try {
            long startTime = System.currentTimeMillis();
            producer.produceBothDatasets(locationCsvPath, evDataCsvPath, maxMessagesPerTopic);
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Producer completed in " + duration + "ms");
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void runConsumer(int maxMessages) {
        System.out.println("\n--- Starting Consumer ---");
        System.out.println("Max Messages: " + (maxMessages == 0 ? "Unlimited" : maxMessages));
        System.out.println("Topics: " + EVDataProducer.TOPIC_LOCATION + ", " + EVDataProducer.TOPIC_EVDATA);
        System.out.println("Consumption order: LOCATION first, then EVDATA.\n");
        if (maxMessages == 0) {
            System.out.println("(Auto-stops after a few seconds of no new records; Ctrl+C can stop immediately)\n");
        } else {
            System.out.println("(Auto-stops after a few seconds when no more records are available, or when limit is reached; Ctrl+C can stop immediately)\n");
        }

        EVDataConsumer locationConsumer = EVDataConsumer.createLocationConsumer();
        try {
            System.out.println("--- Consuming LOCATION topic ---");
            locationConsumer.consume(maxMessages);
        } finally {
            locationConsumer.close();
        }

        EVDataConsumer evDataConsumer = EVDataConsumer.createEVDataConsumer();
        try {
            System.out.println("\n--- Consuming EVDATA topic ---");
            evDataConsumer.consume(maxMessages);
        } finally {
            evDataConsumer.close();
        }
    }

    private static void resetTopicBeforeRerun() {
        System.out.println("\n--- Reset Topic Before Rerun ---");
        System.out.println("Topics: " + String.join(", ", TOPICS));

        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaConfig.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            for (String topic : TOPICS) {
                if (adminClient.listTopics().names().get().contains(topic)) {
                    adminClient.deleteTopics(Collections.singletonList(topic)).all().get();
                    System.out.println("Deleted existing topic: " + topic);
                } else {
                    System.out.println("Topic does not exist, creating a fresh one: " + topic);
                }
            }

            for (String topic : TOPICS) {
                NewTopic newTopic = new NewTopic(topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR);
                try {
                    adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw e;
                    }
                }
                System.out.println("Created topic: " + topic + " (partitions=" + DEFAULT_PARTITIONS + ")");
            }

            System.out.println("Topic reset complete. You can rerun producer now.");
        } catch (Exception e) {
            System.err.println("Failed to reset topic: " + e.getMessage());
            e.printStackTrace();
        }
    }
}