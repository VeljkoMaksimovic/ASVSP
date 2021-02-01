package first_test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Producer {

    public static void main(String[] args) {
        //Producer properties
        Producer producer = new Producer();

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String, String> kafka_producer = new KafkaProducer<>(properties);

        //create a producer record
        producer.sendOneFile(kafka_producer);
        kafka_producer.flush();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        producer.sendMultipleFiles(kafka_producer, 10);
        kafka_producer.flush();

        try {
            br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer.sendMultipleFiles(kafka_producer, 1000);
        kafka_producer.flush();
        //send data - happens asynchronously, does not send right away
        //kafka_producer.send(record);

        //sends everything that was in the buffer
        kafka_producer.flush();

        //sends everything that was in the buffer, and closes the producer.
        kafka_producer.close();
    }

    public Producer(){}

    private void sendOneFile(KafkaProducer<String, String> kafka_producer){
        String file = "";

        try{
            file = new String(Files.readAllBytes(Paths.get("/home/veljko/Downloads/C#training/_1909CustomRenderer.cs")));
        }catch (Exception e){
            e.printStackTrace();
        }

        ProducerRecord<String, String> record = new ProducerRecord<>("kafka-streaming-file-input", "_1909CustomRenderer.cs", file);
        kafka_producer.send(record);
    }

    private void sendMultipleFiles(KafkaProducer<String, String> kafka_producer, int noOfFiles){
        String processedFile = "";

        try{
            File dir = new File("/home/veljko/Downloads/C#training");
            File[] files = dir.listFiles();
            int i = 0;
            for(File file:files) {
                if (i == noOfFiles) break;
                processedFile = new String(Files.readAllBytes(Paths.get(file.getAbsolutePath())));
                ProducerRecord<String, String> record = new ProducerRecord<>("kafka-streaming-file-input", file.getName(), processedFile);
                kafka_producer.send(record);
                i++;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
