package kafka_study;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Properties;
import java.util.Random;

/**
 * @author bystander
 * @date 2020/2/22
 */
public class KafkaProducerTest implements Runnable {

    private static String[] ROLE_ID_ARRAY = {"ROLE001", "ROLE002", "ROLE003", "ROLE004", "ROLE005"};
    private static String[] REGION_ID_ARRAY = {"REG001", "REG002", "REG003", "REG004", "REG005"};
    private static  Integer MAX_USER_AGE = 60;
    //how many records to be generated
    private int MAX_RECORDS = 10000;

    private static KafkaProducer<String, String> producer;
    private final String topic;
    public KafkaProducerTest(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.101.162:9092,10.0.101.162:9093,10.0.101.162:9094");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    private static String getRandomGener() {
        Random rand = new Random();
        int randNum = rand.nextInt(2) + 1;

        if (randNum % 2 == 0) {
            return "M";
        }else{
            return "F";
        }
    }

    private static void GeneratorDataToKafka(String topic, Integer recordNum) {
        for (int i = 0; i < recordNum; i++) {
            String gender = getRandomGener();
            Random rand = new Random();
            int age = rand.nextInt(MAX_USER_AGE);
            if (age < 10) {
                age = age+10;
            }

            int year =  rand.nextInt(16) + 2000;
            int month = rand.nextInt(12) + 1;
            int day = rand.nextInt(28) + 1;
            String registerDate = new StringBuilder(year).append(month).append(day).toString();

            int roleIndex = rand.nextInt(ROLE_ID_ARRAY.length);
            String role = ROLE_ID_ARRAY[roleIndex];
            int regionIndex = rand.nextInt(REGION_ID_ARRAY.length);
            String region = REGION_ID_ARRAY[regionIndex];

            User user = new User();
            user.setAge(age);
            user.setGender(gender);
            user.setRegion(region);
            user.setRegisterDate(registerDate);
            user.setRole(role);
            user.setId(i);

            String jsonUser = JSON.toJSONString(user);

            producer.send(new ProducerRecord<String, String>(topic, String.valueOf(i), jsonUser));


        }
    }

    @Override
    public void run() {
        try {
            GeneratorDataToKafka("public",MAX_RECORDS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    static class User{
        int id;
        String gender;
        int age;
        String registerDate;
        String role;
        String region;

        public User() {

        }

        public User(int id, String gender, int age, String registerDate, String role, String region) {
            this.id = id;
            this.gender = gender;
            this.age = age;
            this.registerDate = registerDate;
            this.role = role;
            this.region = region;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getRegisterDate() {
            return registerDate;
        }

        public void setRegisterDate(String registerDate) {
            this.registerDate = registerDate;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }
    }

    public static void main(String args[]) {
        KafkaProducerTest test = new KafkaProducerTest("public");
        Thread thread = new Thread(test);
        thread.start();
    }
}