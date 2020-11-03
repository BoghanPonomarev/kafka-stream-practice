package com.ponomarev.part3;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {

    private static final String BANK_TRANSACTION_TOPIC = "bank-transaction";

    private Properties producerConfig = new Properties();

    public BankTransactionsProducer() {
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, "3");
        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Only from Kafka 0.11
    }

    public void startProducing() throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig);

        while(true) {
            producer.send(createTransactionOperationRecord("John"));
            Thread.sleep(100);
            producer.send(createTransactionOperationRecord("Alice"));
            Thread.sleep(100);
            producer.send(createTransactionOperationRecord("Bob"));
            Thread.sleep(100);
            producer.flush();
        }
    }

    /**
     * The word 'transaction' I use only because of task domain semantic, this is not kafka transaction!
     */
    private ProducerRecord<String, String> createTransactionOperationRecord(String transactionOwnerName) {
        ObjectNode transactionOperationContent = JsonNodeFactory.instance.objectNode();

        Integer transactionAmount = ThreadLocalRandom.current().nextInt(-100, 100);
        transactionOperationContent.put("name", transactionOwnerName);
        transactionOperationContent.put("amount", transactionAmount);
        transactionOperationContent.put("time", Instant.now().toString());

        return new ProducerRecord<>(BANK_TRANSACTION_TOPIC, transactionOwnerName, transactionOperationContent.toPrettyString());
    }
}
