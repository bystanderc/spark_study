package designPattern.AbstractFactory;

/**
 * @Author baochen.cai
 * @Description concrete factory
 * @Date 2021/2/19
 */
public class ConcreteFactory1 extends AbstractFactory {
    @Override
    AbstractProductA createProductA() {
        return new ProductA1();
    }

    @Override
    AbstractProductB createProductB() {
        return new ProductB1();
    }
}
