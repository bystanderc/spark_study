package kafka_study;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author bystander
 * @date 2020/2/22
 */
public class CanalFromKafka implements Runnable {

    private static final String GROUPID = "groupC";
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private ConsumerRecords<String, String> msgList;

    public CanalFromKafka(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.7:9092,192.168.1.7:9093,192.168.1.7:9094");
        props.put("group.id", GROUPID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<String, String>(props);
        this.topic = topicName;
        this.consumer.subscribe(Arrays.asList(topic));
    }

    public static void main(String args[]) {
        CanalFromKafka kc = new CanalFromKafka("leyou");
//        KafkaConsumerTest test1 = new KafkaConsumerTest("partopic");
        Thread thread1 = new Thread(kc);

        thread1.start();
    }

    /**
     * parse json
     *
     * @param data
     * @return
     */
    private static String parseJson(String data) {
        StringBuffer bf = new StringBuffer();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(data);
            String dataValue = jsonNode.get("data").asText();
            bf.append(dataValue);
            return dataValue;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bf.toString();
    }

    @Override
    public void run() {
        int messageNo = 1;
        System.out.println("---------开始消费---------");
        try {
            for (; ; ) {
                msgList = consumer.poll(1000);
                if (null != msgList && msgList.count() > 0) {
                    for (ConsumerRecord<String, String> record : msgList) {
                        //消费100条就打印 ,但打印的数据不一定是这个规律的
                        //System.out.println(messageNo + "=======receive: key = " + record.key() + ", value = " + record.value() + " offset===" + record.offset());


                        ObjectMapper mapper = new ObjectMapper();

                        JsonNode jsonNode = mapper.readTree(record.value());

                        JsonNode jsonNode2 = jsonNode.path("data").path(0);
                        String database = jsonNode.path("database").asText();
                        String table = jsonNode.path("table").asText();
                        String type = jsonNode.path("type").asText();

                        TbUser tbUser = new TbUser();
                        tbUser.setId(jsonNode2.get("id").asLong());
                        tbUser.setUsername(jsonNode2.get("username").asText());
                        tbUser.setPassword(jsonNode2.get("password").asText());
                        tbUser.setPhone(jsonNode2.get("phone").asText());
                        tbUser.setCreated(jsonNode2.get("created").asText());
                        tbUser.setSalt(jsonNode2.get("salt").asText());

                        System.out.println(tbUser);
                        System.out.println(database);
                        System.out.println(table);
                        System.out.println(type);

                        System.out.println(record.offset());
                    }
                } else {
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
