package kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.KafkaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
public class PublicationsProducer {

    public static AtomicInteger seq = new AtomicInteger(0);

    public static void main(String args[]) throws IOException, URISyntaxException {
        //arg[0] --> HDFS path.....
        // data/publications19.json

        KafkaConfig kafkaConfig = KafkaConfig.create(args[0]);

        //Stream<String> lines = Files.lines(Paths.get("publications19.json"));
        Stream<String> lines;

        System.out.println("Reading from local " + kafkaConfig.getProducerSourceFilePath());
        if(!kafkaConfig.isFromHdfs()) {
            lines = Files.lines(Paths.get(kafkaConfig.getProducerSourceFilePath()));
        } else {
            System.out.println("Reading from hdfs " + kafkaConfig.getProducerSourceFilePath());
            Configuration conf = new Configuration();
            conf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoop/conf/hdfs-site.xml"));
            FileSystem hdfs = FileSystem.get(new URI("hdfs://10.19.8.199:8020"), conf);
            org.apache.hadoop.fs.Path path=new org.apache.hadoop.fs.Path(kafkaConfig.getProducerSourceFilePath());
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
            List<String> readLines = new LinkedList<>();
            try {
                String line;
                line=br.readLine();
                while (line != null){
                    readLines.add(line);
                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                }
                System.out.println("Read publication: " + readLines.size());
                lines = readLines.stream();
            } finally {
                // you should close out the BufferedReader
                br.close();
            }
        }

        //    String topicName = "geofil_publications";
        String topicName = kafkaConfig.getProducerTopic(); // testing --> geofil_results

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.getProducerBroker());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        //props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, String> producer = new KafkaProducer<>(props);

//        publications.forEach(publication -> {
//            try {
//                String value = publication.toString();
//                int id = seq.incrementAndGet();
//                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, String.valueOf(id), value);
//                System.out.println(record);
//                producer.send(record);
//            } catch (Exception e) {
//                System.out.println("Failed to serialize");
//                throw new RuntimeException(e.getMessage());
//            }
//        });

        AtomicInteger counter = new AtomicInteger();
        lines.forEach(line -> {
            try {
                int id = seq.incrementAndGet();
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, String.valueOf(id), line);
                producer.send(record);
                System.out.println("Sent " + counter.getAndIncrement());
            } catch (Exception e) {
                System.out.println("Failed to serialize");
                throw new RuntimeException(e.getMessage());
            }
        });

        producer.flush();
        producer.close();
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R, E extends Throwable> {

        R apply(T t) throws E;

        static <T, R, E extends Throwable> Function<T, R> unchecked(ThrowingFunction<T, R, E> f) {
            return t -> {
                try {
                    return f.apply(t);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}
