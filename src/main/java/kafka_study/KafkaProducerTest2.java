//package kafka_study;
//
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.util.Properties;
//
///**
// * @author bystander
// * @date 2020/2/22
// */
//public class KafkaProducerTest2  {
//
//    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.readTextFile("/Users/bystander/Downloads/SogouE.txt").print();
//
////        Properties props = new Properties();
////        props.put("bootstrap.servers", "192.168.1.7:9092，192.168.1.7:9093，192.168.1.7:9094");
////        props.put("acks", "all");
////        props.put("retries", 0);
////        props.put("batch.size", 16384);
////        props.put("key.serializer", StringSerializer.class.getName());
////        props.put("value.serializer", StringSerializer.class.getName());
////        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
////
////        producer.send(new ProducerRecord<>("Sogou","message",file.toString()));
//
////        producer.close();
//
////        System.out.println("发送的数据为"+file.toString());
//
//
//    }
//}