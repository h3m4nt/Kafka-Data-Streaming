package com.example.kafka.streaming.main;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class KafkaJavaProducer {

    private static final Logger logger = LogManager.getLogger(KafkaJavaProducer.class);

    public static void main(String[] args) {
        String topicName;
        String fileName;

        if (args.length != 2) {
            logger.error("Please provide command line arguments: topicName FilePath");
            System.exit(-1);
        }
        topicName = args[0];
        fileName = args[1];
        logger.info("Starting Producer...");
        logger.debug("topicName=" + topicName + ", fileName=" + fileName);
        logger.debug("Creating Kafka Producer...");

        //setup basic kafka properties
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaJavaProducer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        Producer producer = new KafkaProducer(props);

        logger.debug("Start sending messages...");
        //Reading a file and writing into kafka topic
        try(BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)))) {
            String line = reader.readLine();
            int i = 0;
            while(line != null){
                producer.send(new ProducerRecord<>(topicName, i++, line));
                line = reader.readLine();
            }
        } catch (Exception e) {
            logger.error("Exception occurred – Check log for more details.\n" + e.getMessage());
            System.exit(-1);
        } finally {
            logger.info("Finished KafkaJavaProducer – Closing Kafka Producer.");
            producer.close();
        }
        System.out.println("Completed");
    }
}
