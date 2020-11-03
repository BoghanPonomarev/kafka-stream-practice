package com.ponomarev.part3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;

public class BankTransactionConsumer {

    private static final String BANK_TRANSACTION_TOPIC = "bank-transaction";

    private Properties consumerConfig = new Properties();
    private StreamsBuilder streamsBuilder = new StreamsBuilder();

    public BankTransactionConsumer() {
        consumerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-transaction-application");
        consumerConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    }

    public void consume() {
        Serde<JsonNode> valueSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
        KStream<String, JsonNode> bankTransactionsStream = streamsBuilder.stream(BANK_TRANSACTION_TOPIC, Consumed.with(Serdes.String(), valueSerde));

        bankTransactionsStream
                .peek((key, value) -> System.out.println("NEW MESSAGE: K - " + key + " V - " + value))
                .groupByKey(Grouped.with(Serdes.String(), valueSerde))
                .aggregate(this::crateDefaultUserAccount, this::recalculateBalance, Materialized.with(Serdes.String(), valueSerde))
                .toStream()
                .to( "bank-balance", Produced.with(Serdes.String(), valueSerde));

        Topology bankTransactionTopology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(bankTransactionTopology, consumerConfig);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private JsonNode crateDefaultUserAccount()  {
        ObjectNode defaultUserValue = JsonNodeFactory.instance.objectNode();
        defaultUserValue.put("name", "none");
        defaultUserValue.put("count", 0);
        defaultUserValue.put("balance", 0);
        defaultUserValue.put("lastTransactionTime", Instant.ofEpochMilli(0L).toString());
        return defaultUserValue;
    }

    private JsonNode recalculateBalance(String key, JsonNode transaction, JsonNode currentBalance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("name", key);
        newBalance.put("count", currentBalance.get("count").asInt() + 1);
        newBalance.put("balance", currentBalance.get("balance").asInt() + transaction.get("amount").asInt());
        newBalance.put("lastTransactionTime", transaction.get("time").asText());

       return newBalance;
    }

}
