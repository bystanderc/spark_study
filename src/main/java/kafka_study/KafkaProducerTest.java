package kafka_study;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author bystander
 * @date 2020/2/22
 */
public class KafkaProducerTest implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    public KafkaProducerTest(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.7:9092，192.168.1.7:9093，192.168.1.7:9094");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    @Override
    public void run() {
        int messageNo = 1;
        int i = 1;
        try {
            while(true) {
                String messageStr="你好，这是第"+i+"条数据";
                producer.send(new ProducerRecord<String, String>(topic, "Message", messageStr));

                Thread.sleep(1000);

                //生产了100条就打印


                if(i%100==0){
                    System.out.println("发送的信息:" + messageStr);
                }
                //生产1000条就退出
                if(i%11000==0){
                    System.out.println("成功发送了"+messageNo+"条");
                }
                messageNo++;
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String args[]) {
        KafkaProducerTest test = new KafkaProducerTest("partopic");
        Thread thread = new Thread(test);
        thread.start();
    }
}