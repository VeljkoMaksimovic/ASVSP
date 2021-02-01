package streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class PreprocessFileInput {

    public static void main(String[] args) {

        PreprocessFileInput preprocessor = new PreprocessFileInput();

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "preprocessFile");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //stream from kafka
        KStream<String, String> fileInput = builder.stream("kafka-streaming-file-input");

        KStream<String, String> preprocessedFile = fileInput
                .mapValues(value -> preprocessor.processFile(value));

        preprocessedFile.to("kafka-streaming-file-output", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    String processFile(String file){
        List<String> lines = new LinkedList<>(Arrays.asList(file.split("\n")));
        int curIndex = 0;
        while (curIndex < lines.size()){
            if (lines.get(curIndex).length()==0 ||
                    lines.get(curIndex).contains("using") ||
                    lines.get(curIndex).trim().startsWith("//")){
                lines.remove(curIndex);
                curIndex--;
            }
            curIndex++;
        }
        return String.join("\n", lines);
    }
}
