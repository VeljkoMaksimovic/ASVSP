package streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class CountingImports {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "preprocess_file");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        //stream from kafka
        KStream<String, String> filesInput = builder.stream("kafka-streaming-file-input");

        KTable<String, Long> importsCount = null;
        try {
            importsCount = filesInput
                    .flatMapValues(value -> Arrays.asList(value.split("\n")))
                    .filter((key, value) -> value.trim().startsWith("using"))
                    //deleting the "using" part and keeping only the name of the import
                    .mapValues(value -> value.split(" ")[value.split(" ").length-1])
                    .selectKey((oldKey, value) -> value)
                    .groupByKey()
                    .count();
        }catch(Exception e){
            e.printStackTrace();
        }

        importsCount.toStream().to("import-usage-counts", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
