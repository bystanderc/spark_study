package designPattern.AbstractFactory;

/**
 * @Author baochen.cai
 * @Description concrete factory
 * @Date 2021/2/19
 */
public class ConcreteFactory2 extends AbstractFactory {
    @Override
    AbstractProductA createProductA() {
        return new ProductA2();
    }

    @Override
    AbstractProductB createProductB() {
        return new ProductB2();
    }
}
