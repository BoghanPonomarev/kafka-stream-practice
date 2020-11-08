package com.ponomarev.part1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * 1. Stream from kafka
 * 2. Log it
 * 3. Map values to lowercase
 * 4. Flat map values, split by space
 * 5. Select key from value
 * 6. Group by key
 * 7. Count occurrences
 * 8. Push result back into kafka
 */
public class WordCount {

    private static final String WORD_COUNT_TOPIC = "word-count-input";

    private Properties config = new Properties();
    private StreamsBuilder streamsBuilder = new StreamsBuilder();

    public WordCount() {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    public void execute() {
        Topology topology = createTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, config);
        kafkaStreams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public Topology createTopology() {
        KStream<String, String> stream = streamsBuilder.stream(WORD_COUNT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        stream.peek((key, value) -> System.out.println("NEW MESSAGE: K - " + key + " V - " + value))
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((key, value) -> value)
                .groupByKey()
                .count(Named.as("Counts"))
                .toStream()
                .to("word-count-output");

        return streamsBuilder.build();
    }

}
