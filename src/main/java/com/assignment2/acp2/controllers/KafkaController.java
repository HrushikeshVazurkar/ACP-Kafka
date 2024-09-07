package com.assignment2.acp2.controllers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.*;

@RestController
public class KafkaController {

    private static void KafkaConnectionVerification(Properties properties, String topicName) throws Exception {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));
        KafkaFuture<Map<String, TopicDescription>> future = describeTopicsResult.all();
        future.get(); // Exception if the topic doesn't exist
    }

    private static Properties KafkaProperties(){

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "pkc-41mxj.uksouth.azure.confluent.cloud:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='UPEN4GDPF7QOGVKQ' password='XIJspRZGbFQa8+LXOEJXM4LULy1iOyfL4VrnddRSKQx79d/p0BpS4gmXhsetjMD1';");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "group_test");
        properties.put("storage.server", "https://acp-storage.azurewebsites.net/");

        return properties;
    }

    private static ResponseEntity<String> KafkaReadOperation(String topicName, Properties properties){

        try {
            KafkaConnectionVerification(properties, topicName);

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            ArrayList<String> stringList = new ArrayList<>();

            consumer.subscribe(Collections.singletonList(topicName));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            System.out.println("records printed " + records.toString());
            if (records.isEmpty()) {
                consumer.close();
//                stringList.add("No unread records.");
                return ResponseEntity.status(404).body("No unread records.");
            } else {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("value " + record.value());
                    stringList.add(record.value());
                }
            }
            consumer.close();
            return ResponseEntity.status(200).body(stringList.get(stringList.size() - 1));
        }
        catch (Exception e){
            return ResponseEntity.status(400).body("Error in reading data from Kafka Topic.");
        }
    }

    private static ResponseEntity<String> KafkaWriteOperation(String topicName, Properties properties, String data){

        try {
            KafkaConnectionVerification(properties, topicName);

            Producer<String, String> producer = new KafkaProducer<>(properties);
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "s2550941", data);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("Message sent successfully to topic: " + metadata.topic() +
                                ", partition: " + metadata.partition() +
                                ", offset: " + metadata.offset());
                    } else {
                        System.err.println("Error sending message: " + exception.getMessage());
                    }
                }
            });
            producer.close();

            return ResponseEntity.status(200).body("Wrote " + data + " to " + topicName + ".");
        } catch (Exception e){
            return ResponseEntity.status(400).body("Kafka write failed.");
        }
    }

    public static ResponseEntity<String> BlobWriteOperation(String url, Map<String, String> payload){
        try {
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<String> response = restTemplate.postForEntity(url, payload, String.class);
            return ResponseEntity.status(200).body(response.getBody());
        } catch (Exception e){
            return ResponseEntity.status(400).body("Blob write failed.");
        }
    }

    public static ResponseEntity<String> BlobReadOperation(String url, String UUID){
        try {
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

            if (response.getStatusCode().is2xxSuccessful())
                return ResponseEntity.status(200).body(response.getBody());
            return ResponseEntity.status(400).body(response.getBody());
        } catch (Exception e){
            return ResponseEntity.status(400).body("Blob read failed.");
        }
    }

    @PostMapping("/readTopic/{topicName}")
    public ResponseEntity<String> readTopic(@PathVariable String topicName, @RequestBody(required = false) List<Map<String, String>> overrideConfigParams) {
        Properties properties = KafkaProperties();

        if(overrideConfigParams != null) {
            for (Map<String, String> obj : overrideConfigParams) {
                properties.putAll(obj);
            }
        }

        ResponseEntity<String> latest_unread_message = KafkaReadOperation(topicName, properties);

        if(latest_unread_message.getStatusCode().is2xxSuccessful())
            return ResponseEntity.status(200).body(latest_unread_message.getBody());
        return ResponseEntity.status(400).body(latest_unread_message.getBody());
    }

    @PostMapping("/writeTopic/{topicName}/{data}")
    public ResponseEntity<String> writeTopic(@PathVariable String topicName, @PathVariable String data, @RequestBody(required = false) List<Map<String, String>> overrideConfigParams) {
        Properties properties = KafkaProperties();

        if(overrideConfigParams != null) {
            for (Map<String, String> obj : overrideConfigParams) {
                properties.putAll(obj);
            }
        }

        ResponseEntity<String> status = KafkaWriteOperation(topicName, properties, data);

        if(status.getStatusCode().is2xxSuccessful())
            return ResponseEntity.status(200).body(status.getBody());
        return ResponseEntity.status(400).body(status.getBody());
    }

    @PostMapping("/transformMessage/{readTopic}/{writeTopic}")
    public ResponseEntity<String> transformMessage(@PathVariable String readTopic, @PathVariable String writeTopic, @RequestBody(required = false) List<Map<String, String>> overrideConfigParams){
        Properties properties = KafkaProperties();

        if(overrideConfigParams != null) {
            for (Map<String, String> obj : overrideConfigParams) {
                properties.putAll(obj);
            }
        }

        // check if writeTopic is correct
        try{
            KafkaConnectionVerification(properties, writeTopic);

            ResponseEntity<String> latest_unread_message = KafkaReadOperation(readTopic, properties);

            if(latest_unread_message.getStatusCode().is2xxSuccessful()){
                ResponseEntity<String> status = KafkaWriteOperation(writeTopic, properties, latest_unread_message.getBody().toUpperCase());
                if(status.getStatusCode().is2xxSuccessful())
                    return ResponseEntity.status(200).body(status.getBody());
                return ResponseEntity.status(400).body(status.getBody());
            }
            else
                return ResponseEntity.status(400).body(latest_unread_message.getBody());
        } catch (Exception e){
            return ResponseEntity.status(400).body("Incorrect write topic.");
        }

    }

    @PostMapping("/store/{readTopic}/{writeTopic}")
    public ResponseEntity<String> store(@PathVariable String readTopic, @PathVariable String writeTopic, @RequestBody(required = false) List<Map<String, String>> overrideConfigParams) throws Exception{

        Properties properties = KafkaProperties();

        if(overrideConfigParams != null) {
            for (Map<String, String> obj : overrideConfigParams) {
                properties.putAll(obj);
            }
        }

        try {
            KafkaConnectionVerification(properties, writeTopic);

            ResponseEntity<String> latest_unread_message = KafkaReadOperation(readTopic, properties);
            if (latest_unread_message.getStatusCode().is2xxSuccessful()) {
                Map<String, String> payload = Map.of(
                        "uid", "s2550941",
                        "datasetName", "dataset1",
                        "data", latest_unread_message.getBody()
                );

                String url = properties.getProperty("storage.server") + "/write/blob";

                ResponseEntity<String> UUID = BlobWriteOperation(url, payload);

                if (UUID.getStatusCode().is2xxSuccessful()) {
                    ResponseEntity<String> status = KafkaWriteOperation(writeTopic, properties, UUID.getBody());
                    if (status.getStatusCode().is2xxSuccessful())
                        return ResponseEntity.status(200).body(status.getBody());
                    return ResponseEntity.status(400).body(status.getBody());
                } else
                    return ResponseEntity.status(400).body(UUID.getBody());
            } else {
                return ResponseEntity.status(400).body(latest_unread_message.getBody());
            }
        } catch (Exception e){
            return ResponseEntity.status(400).body("Incorrect write topic.");
        }

    }

    @PostMapping("/retrieve/{writeTopic}/{uuid}")
    public ResponseEntity<String> retrieve(@PathVariable String writeTopic, @PathVariable String uuid, @RequestBody(required = false) List<Map<String, String>> overrideConfigParams) throws Exception {

        Properties properties = KafkaProperties();

        if(overrideConfigParams != null) {
            for (Map<String, String> obj : overrideConfigParams) {
                properties.putAll(obj);
            }
        }

        String url = properties.getProperty("storage.server") + "/read/blob/" + uuid;
        ResponseEntity<String> dataFromUUID = BlobReadOperation(url, uuid);

        if(dataFromUUID.getStatusCode().is2xxSuccessful()){
            ResponseEntity<String> status = KafkaWriteOperation(writeTopic, properties, dataFromUUID.getBody());
            if(status.getStatusCode().is2xxSuccessful())
                return ResponseEntity.status(200).body(status.getBody());
            return ResponseEntity.status(400).body(status.getBody());
        } else {
            return ResponseEntity.status(400).body(dataFromUUID.getBody());
        }

    }
}
