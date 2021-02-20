package designPattern.AbstractFactory;

/**
 * @Author baochen.cai
 * @Description 抽象工厂类
 * @Date 2021/2/19
 */
public abstract class AbstractFactory {
    abstract AbstractProductA createProductA();

    abstract AbstractProductB createProductB();
}
