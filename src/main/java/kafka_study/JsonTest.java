package kafka_study;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * @author bystander
 * @date 2020/4/1
 */
public class JsonTest {

    public static void main(String[] args) throws IOException {
        String s = "{\n" +
                "    \"data\": [{\n" +
                "        \"id\": \"44\",\n" +
                "        \"username\": \"fads\",\n" +
                "        \"password\": \"fasdf\",\n" +
                "        \"phone\": \"fasd\",\n" +
                "        \"created\": \"2020-04-01 09:11:04\",\n" +
                "        \"salt\": \"fasdfasdfasdffsda\"\n" +
                "    }],\n" +
                "    \"database\": \"leyou\",\n" +
                "    \"es\": 1585704571000,\n" +
                "    \"id\": 4,\n" +
                "    \"isDdl\": false,\n" +
                "    \"mysqlType\": {\n" +
                "        \"id\": \"bigint(20)\",\n" +
                "        \"username\": \"varchar(32)\",\n" +
                "        \"password\": \"varchar(32)\",\n" +
                "        \"phone\": \"varchar(11)\",\n" +
                "        \"created\": \"datetime\",\n" +
                "        \"salt\": \"varchar(32)\"\n" +
                "    },\n" +
                "    \"old\": [{\n" +
                "        \"salt\": \"fasdfasdfasdf\"\n" +
                "    }],\n" +
                "    \"pkNames\": [\"id\"],\n" +
                "    \"sql\": \"\",\n" +
                "    \"sqlType\": {\n" +
                "        \"id\": -5,\n" +
                "        \"username\": 12,\n" +
                "        \"password\": 12,\n" +
                "        \"phone\": 12,\n" +
                "        \"created\": 93,\n" +
                "        \"salt\": 12\n" +
                "    },\n" +
                "    \"table\": \"tb_user\",\n" +
                "    \"ts\": 1585704571984,\n" +
                "    \"type\": \"UPDATE\"\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();

        JsonNode jsonNode = mapper.readTree(s);

        JsonNode jsonNode2 = jsonNode.path("data").path(0);


        TbUser tbUser = new TbUser();
        tbUser.setId(jsonNode2.get("id").asLong());
        tbUser.setUsername(jsonNode2.get("username").asText());
        tbUser.setPassword(jsonNode2.get("password").asText());
        tbUser.setPhone(jsonNode2.get("phone").asText());
        tbUser.setCreated(jsonNode2.get("created").asText());
        tbUser.setSalt(jsonNode2.get("salt").asText());

        System.out.println(tbUser);


    }
}
