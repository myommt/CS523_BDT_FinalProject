# Real-Time Data Analytics Pipeline  
Kafka → Spark → Hive → Tableau

## 📌 Project Overview

This project demonstrates a complete real-time big data analytics pipeline using:

- **Apache Kafka** for data streaming
- **Apache Spark** for stream processing
- **Apache Hive** for persistent storage and querying
- **Tableau** for visualization and dashboarding

The pipeline processes streaming data from Kafka, transforms it using Spark Structured Streaming, stores the processed data into Hive tables, and visualizes insights through Tableau dashboards.

---

## 🏗️ Architecture

```text
Producer Application
        │
        ▼
   Apache Kafka
        │
        ▼
 Apache Spark Streaming
        │
        ▼
    Apache Hive
        │
        ▼
      Tableau

```

## 🔑 Instructions for Kafka and Spark Streaming

Please refer to the `README.md` files within the Kafka and Spark Streaming application folders for detailed setup and execution instructions.
