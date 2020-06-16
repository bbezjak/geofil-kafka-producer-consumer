package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class KafkaConfig {

    // ip_address:port
    private String producerBroker;
    private String producerTopic;
    private boolean fromHdfs;
    private String producerSourceFilePath;

    private String consumerAppId;
    private String consumerClientId;
    // ip_address:port
    private String consumerBroker;
    private String consumerTopic;
    private String consumerDestinationFilePath;

    public static KafkaConfig create(String path) throws IOException {

        System.out.println("Reading config from " + path);
        File config = Paths.get(path).toFile();

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        KafkaConfig kafkaConfig = mapper.readValue(config, KafkaConfig.class);

        System.out.println("Used configuration:");
        System.out.println(kafkaConfig.toString());
        return kafkaConfig;
    }

    public String getProducerBroker() {
        return producerBroker;
    }

    public void setProducerBroker(String producerBroker) {
        this.producerBroker = producerBroker;
    }

    public String getProducerTopic() {
        return producerTopic;
    }

    public void setProducerTopic(String producerTopic) {
        this.producerTopic = producerTopic;
    }

    public boolean isFromHdfs() {
        return fromHdfs;
    }

    public void setFromHdfs(boolean fromHdfs) {
        this.fromHdfs = fromHdfs;
    }

    public String getProducerSourceFilePath() {
        return producerSourceFilePath;
    }

    public void setProducerSourceFilePath(String producerSourceFilePath) {
        this.producerSourceFilePath = producerSourceFilePath;
    }

    public String getConsumerAppId() {
        return consumerAppId;
    }

    public void setConsumerAppId(String consumerAppId) {
        this.consumerAppId = consumerAppId;
    }

    public String getConsumerClientId() {
        return consumerClientId;
    }

    public void setConsumerClientId(String consumerClientId) {
        this.consumerClientId = consumerClientId;
    }

    public String getConsumerBroker() {
        return consumerBroker;
    }

    public void setConsumerBroker(String consumerBroker) {
        this.consumerBroker = consumerBroker;
    }

    public String getConsumerTopic() {
        return consumerTopic;
    }

    public void setConsumerTopic(String consumerTopic) {
        this.consumerTopic = consumerTopic;
    }

    public String getConsumerDestinationFilePath() {
        return consumerDestinationFilePath;
    }

    public void setConsumerDestinationFilePath(String consumerDestinationFilePath) {
        this.consumerDestinationFilePath = consumerDestinationFilePath;
    }

    @Override
    public String toString() {
        return "KafkaConfig{" +
                "producerBroker='" + producerBroker + '\'' +
                ", producerTopic='" + producerTopic + '\'' +
                ", fromHdfs=" + fromHdfs +
                ", producerSourceFilePath='" + producerSourceFilePath + '\'' +
                ", consumerAppId='" + consumerAppId + '\'' +
                ", consumerClientId='" + consumerClientId + '\'' +
                ", consumerBroker='" + consumerBroker + '\'' +
                ", consumerTopic='" + consumerTopic + '\'' +
                ", consumerDestinationFilePath='" + consumerDestinationFilePath + '\'' +
                '}';
    }
}
