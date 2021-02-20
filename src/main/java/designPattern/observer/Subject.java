package designPattern.observer;

/**
 * @author bystander
 * @date 2021/2/19
 */
public interface Subject {

    void registerObserver(Observer o);

    void removeObserver(Observer o);

    void notifyObserver();
}
