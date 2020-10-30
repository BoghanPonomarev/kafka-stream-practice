package com.ponomarev.part2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Properties;

public class FavouriteColorCount {

    private final static String USER_COLORS_INPUT_STREAM = "user-colors-input";
    private final static String USER_COLORS_OUTPUT_STREAM = "user-colors-output";


    private Properties config = new Properties();
    private StreamsBuilder streamsBuilder = new StreamsBuilder();

    public FavouriteColorCount() {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    /**
     * Variant with keys, see materials in description folder.
     */
    public void executeWithKeys() {
        KStream<String, String> userColorsStream = streamsBuilder.stream(USER_COLORS_INPUT_STREAM, Consumed.with(Serdes.String(), Serdes.String()));

        userColorsStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .filter((key, value) -> value.equals("green") || value.equals("blue") || value.equals("red"))
                .map((key, value) -> KeyValue.pair(value, key))
                .groupByKey()
                .count(Named.as("color-count"))
                .toStream()
                .to(USER_COLORS_OUTPUT_STREAM);

        Topology userColorTopology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(userColorTopology, config);
        kafkaStreams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    /**
     * Variant with simulated keys, real keys are nulls, see materials in description folder.
     */
    public void executeWhenNoKeys() {
        KStream<String, String> userColorsStream = streamsBuilder.stream(USER_COLORS_INPUT_STREAM, Consumed.with(Serdes.String(), Serdes.String()));

        userColorsStream
                .filter((key, value) -> value.matches("\\w+,\\w+"))
                .map((key, value) -> KeyValue.pair(value.split(",")[1], value.split(",")[0]))
                .filter((key, value) -> key.equals("green") || key.equals("blue") || key.equals("red"))
                .groupByKey()
                .count(Named.as("color-count"))
                .toStream()
                .to(USER_COLORS_OUTPUT_STREAM);

        Topology userColorTopology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(userColorTopology, config);
        kafkaStreams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

}
