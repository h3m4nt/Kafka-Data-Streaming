package com.example.kafka.streaming.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaJavaConsumer {

    private static final Logger logger = LogManager.getLogger(KafkaJavaConsumer.class);

    public static void main(String[] args) {
        String topicName;

        if (args.length != 1) {
            logger.error("Please provide command line arguments: topicName");
            System.exit(-1);
        }
        topicName = args[0];
        logger.info("Starting Consumer...");
        logger.debug("topicName=" + topicName);

        //setup basic kafka properties
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaJavaConsumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        Map<String, Integer> ipCountMap =  new HashMap<>();
        try {
            //Setting up time interval for Data reading
            LocalDateTime localTime = LocalDateTime.now().plusMinutes(1);
            while(true) {
                //System.out.println("Running Consumer at : " + LocalDateTime.now());
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(10));
                //store streaming data to process after a certain time
                storeData(ipCountMap, records);
                consumer.commitAsync();

                //Running at every minute to find out DDOS Attack
                if(LocalDateTime.now().isAfter(localTime)) {
                    localTime = processData(localTime, ipCountMap);
                }
            }

        } catch (Exception e) {
            logger.error("Exception occurred – Check log for more details.\n" + e.getMessage());
            System.exit(-1);
        } finally {
            logger.info("Finished KafkaConsumer – Closing Kafka Consumer.");
            consumer.close();
        }
        System.out.println("Completed");
    }

    //Store data into map to process after a certain time
    private static void storeData(Map<String, Integer> ipCountMap, ConsumerRecords<Integer, String> records){
        //Iterating over records and store data in map
        for (ConsumerRecord record : records) {
            String ip = record.value().toString().split(" ")[0];
            int count = ipCountMap.getOrDefault(ip, 0);
            ipCountMap.put(ip, count + 1);
        }
    }

    private  static LocalDateTime processData(LocalDateTime localTime, Map<String, Integer> ipCountMap){
        System.out.println("Reading map at : " + localTime);
        ipCountMap.forEach((ip, count) -> {
            if(count > 50)
                System.out.println(ip + " ===> " + count);
        });
        ipCountMap.clear();
        return localTime.plusMinutes(1);
    }
}
