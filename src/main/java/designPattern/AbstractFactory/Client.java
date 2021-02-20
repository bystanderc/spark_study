package designPattern.AbstractFactory;

/**
 * @Author baochen.cai
 * @Description client
 * @Date 2021/2/19
 */
public class Client {

    public static void main(String[] args) {
        ConcreteFactory1 abstractFactory = new ConcreteFactory1();
        AbstractProductA productA = abstractFactory.createProductA();
        AbstractProductB productB = abstractFactory.createProductB();
        //do something with productA and productB
    }
}
