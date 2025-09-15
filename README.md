# Spring Boot Kafka Consumer

A Spring Boot based **Apache Kafka Consumer** for large message processing.  
Integrates with **Amazon Selling Partner API (SP-API)** and demonstrates scalable Kafka consumption, batch handling, and containerized deployment.

---

## 🚀 Build and Run

### 1. Install Amazon Selling Partner API Java SDK
First, install the SP-API JAR file into your local Maven repository:

```bash
mvn install:install-file \
  -Dfile=[path to JAR file in "target" folder] \
  -DgroupId=com.amazon.sellingpartnerapi \
  -DartifactId=sellingpartnerapi-aa-java \
  -Dversion=1.0 \
  -Dpackaging=jar

```
### 2. Build the Application

```bash
./mvnw clean install -Dmaven.test.skip=true
```

This will generate an app.jar in the target folder.

### 3. Prepare Docker

Copy the generated JAR file to the Docker app folder:

### 4. Run with Docker Compose

Navigate to the docker/ folder and run:

---

## 📺 Video Explanation
[Watch on YouTube](https://www.youtube.com/watch?v=KzDPROi1BUM)  

---

### 📌 Features
- Spring Boot + Apache Kafka Consumer
- Handles large message processing
- Amazon Selling Partner API integration
- Packaged with Docker & Docker Compose
