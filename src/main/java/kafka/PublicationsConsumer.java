package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import utils.KafkaConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class PublicationsConsumer {

    public static void main(String args[]) throws IOException {

        KafkaConfig kafkaConfig = KafkaConfig.create(args[0]);

        File file = new File(kafkaConfig.getConsumerDestinationFilePath());
        if(file.exists() && !file.isDirectory()) {
            throw new RuntimeException("File " + file.getName() + " already exists");
        }
        FileWriter fw = new FileWriter(file.getName(), true);

        final KafkaStreams streams = createStreams(kafkaConfig.getConsumerBroker(), "/tmp/kafka-streams", kafkaConfig, fw);

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        FileWriter finalFw = fw;
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            try {
                finalFw.flush();
                finalFw.close();
                streams.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    private static KafkaStreams createStreams(final String bootstrapServers,
                                              final String stateDir,
                                              KafkaConfig kafkaConfig,
                                              FileWriter fw) {

        final Properties streamsConfiguration = new Properties();
        // unique app id on kafka cluster
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.getConsumerAppId());
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, kafkaConfig.getConsumerClientId());
        // kafka broker address
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // local state store
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        // consumer from the beginning of the topic or last offset
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // override default serdes
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        String topic = kafkaConfig.getConsumerTopic();

        // Get the stream of station statuses
        KStream<String, String> geometryStream =
                builder.stream(topic,
                Consumed.with(Serdes.String(), Serdes.String()))
                .map((geometry_id, stringValue) -> {
                    try {
                        return new KeyValue<>(geometry_id, stringValue);
                    } catch (Exception e) {
                        throw new RuntimeException("Deserialize error" + e);
                    }
                });

        try {
            fw = new FileWriter(kafkaConfig.getConsumerDestinationFilePath(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileWriter finalFw = fw;
        geometryStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String s, String s2) {
                try {
                    finalFw.append(s + ": avgProcTime " + s2 + "\n");
                    System.out.println(s);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}