package com.thomas.simple.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // create Producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 19; i++) {

            String topic = "second-topic";
            String value = "Hello word" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info("Key: " + key); // log the key
            // id_0 is going partition 1
            // id_1 partition 0
            // id_1 partition 2
            // id_1 partition 0
            // id_1 partition 2
            // id_1 partition 0
            // id_1 partition 2
            // id_1 partition 1
            // id_1 partition 2





            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,key, value);

            // send data - asynchronous
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is throw
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "topic: " + recordMetadata.topic() + "\n" +
                                "partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing ", e);
                    }
                }
            }).get(); // block the .send() to make it synchronous -  don't do this in production!

        }
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();

    }
}
