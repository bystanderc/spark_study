package designPattern.SimpleFactory;

/**
 * @Author baochen.cai
 * @Description 客户端类
 * @Date 2021/2/19
 */
public class Client {

    public static void main(String[] args) {
        SimpleFactory simpleFactory = new SimpleFactory();

        Produce produce = simpleFactory.createProduce(1);

        Produce produce1 = simpleFactory.createProduce(2);
        //do something
    }
}
