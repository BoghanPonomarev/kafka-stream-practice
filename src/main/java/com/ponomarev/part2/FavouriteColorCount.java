package com.ponomarev.part2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class FavouriteColorCount {

    private final static String USER_COLORS_INPUT_STREAM = "user-colors-input";
    private final static String USER_COLORS_OUTPUT_STREAM = "user-colors-output";

    private final static String USER_COLORS_INPUT_STREAM_2 = USER_COLORS_INPUT_STREAM + "-2";
    private final static String USER_COLORS_OUTPUT_STREAM_2 = USER_COLORS_OUTPUT_STREAM + "-2";

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

    /**
     * The option taking into account the previous choices of people, that is,
     * in this option, a person can re-choose a color and one color will have 1 less value,
     * and the other more
     */
    public void executeWithDynamicChooseChange() {
        KStream<String, String> userColorsStream = streamsBuilder.stream(USER_COLORS_INPUT_STREAM_2, Consumed.with(Serdes.String(), Serdes.String()));

        userColorsStream
                .filter((key, value) -> value.matches("\\w+,\\w+"))
                .map((key, value) -> KeyValue.pair(value.split(",")[0], value.split(",")[1]))
                .filter((key, value) -> value.equals("green") || value.equals("blue") || value.equals("red"))
                .to("users-with-colors-table");

        // Intermediate writing and reading from the table to take into account all data
        KTable<String, String> userColorTable = streamsBuilder.table("users-with-colors-table");

        userColorTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Named.as("color-rating"))
                .toStream()
                .to(USER_COLORS_OUTPUT_STREAM_2);

        Topology userColorTopology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(userColorTopology, config);
        kafkaStreams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

}
