package designPattern.observer;

/**
 * @author bystander
 * @date 2021/2/19
 */
public class CurrentConditionsDisplay implements Observer {

    public CurrentConditionsDisplay(Subject weatherData) {

        weatherData.registerObserver(this);
    }

    @Override
    public void update(float temp, float humidity, float presure) {
        System.out.println("CurrentConditionsDisplay.update: " + temp + " " + humidity + " " + presure);
    }
}
