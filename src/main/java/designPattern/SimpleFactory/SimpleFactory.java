package designPattern.SimpleFactory;

/**
 * @Author baochen.cai
 * @Description simple factory class
 * @Date 2021/2/19
 */
public class SimpleFactory {

    public Produce createProduce(int type) {
        if (type == 1) {
            return new ConcreteProduct1();
        }
        if (type == 2) {
            return new ConcreteProduc2();
        }

        return new ConcreteProduct();
    }
}
