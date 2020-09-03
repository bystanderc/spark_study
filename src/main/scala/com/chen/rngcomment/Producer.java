package com.chen.rngcomment;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @author bystander
 * @date 2020/4/25
 */
public class Producer {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();

        //kafka服务器地址
        props.put("bootstrap.servers", "192.168.1.7:9092,192.168.1.7:9093,192.168.1.7:9094");
        //消息确认机制
        props.put("acks", "all");
        //重试机制
        props.put("retries", 0);
        //批量发送的大小
        props.put("batch.size", 16384);
        //消息延迟
        props.put("linger.ms", 1);
        //批量的缓冲区大小
        props.put("buffer.memory", 33554432);
        //kafka数据中key  value的序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        String line = null;

        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("/Users/bystander/IdeaProjects/spark_test2/data/comment/new_rng_comment.csv")));

        int partitioner = 0;

        while ((line = bufferedReader.readLine()) != null) {
            if (Integer.parseInt(line.split(",")[0]) % 2 == 0) {
                partitioner = 1;
            } else {
                partitioner = 2;
            }

            ProducerRecord<String, String> record = new ProducerRecord<>("rng_comment", partitioner, partitioner + "", line);

            kafkaProducer.send(record);
        }

        bufferedReader.close();
        kafkaProducer.close();


    }
}
