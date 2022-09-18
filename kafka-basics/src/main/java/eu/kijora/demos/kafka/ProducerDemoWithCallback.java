package eu.kijora.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Starting...");

        //create producer props
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "Hello world!!!");

        //send data
        for (int i = 0; i < 10; i++) {
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a method is successflully sent or and exc is thrown
                    if (e == null) {
                        log.info("\n\nReceived new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("ERROR: " + e);
                    }
                }
            });
        }

        //flush and close the Producer
        //We have to flush because the program won't wait until the .send() is done and will proceed and close before
        //the producer has a chance to actually  -  synchronous operation!
        producer.flush();

        //also contains flush
        producer.close();

    }
}
