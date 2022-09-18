package eu.kijora.demos.kafka;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

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
        producer.send(producerRecord);

        //flush and close the Producer
        //We have to flush because the program won't wait until the .send() is done and will proceed and close before
        //the producer has a chance to actually  -  synchronous operation!
//        producer.flush();

        //also contains flush
        producer.close();

    }
}
