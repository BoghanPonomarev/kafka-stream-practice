import com.ponomarev.part1.WordCount;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertThat;

public class WordCountAppTest {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    private Serde<String> stringSerde = new Serdes.StringSerde();

    @Before
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCount wordCount = new WordCount();
        topologyTestDriver = new TopologyTestDriver(wordCount.createTopology(), config);

        inputTopic = topologyTestDriver.createInputTopic("word-count-input",
                stringSerde.serializer(), stringSerde.serializer());
        outputTopic = topologyTestDriver.createOutputTopic("word-count-output",
                stringSerde.deserializer(),  new Serdes.LongSerde().deserializer());

    }

    @After
    public void closeTestDriver() {
        topologyTestDriver.close();
    }

    @Test
    public void dummyTest() {
        List<KeyValue<String, Long>> expectedData = Arrays.asList(new KeyValue<>("asd", 1L),
                new KeyValue<>("asd", 2L), new KeyValue<>("asd", 3L));

        inputTopic.pipeInput("asd asd asd");

        assertThat(outputTopic.readKeyValuesToList(), IsEqual.equalTo(expectedData));
    }
}
