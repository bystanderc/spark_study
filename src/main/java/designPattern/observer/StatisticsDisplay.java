package designPattern.observer;

/**
 * @author bystander
 * @date 2021/2/19
 */
public class StatisticsDisplay implements Observer {

    public StatisticsDisplay(Subject weatherData) {

        weatherData.registerObserver(this);
    }

    @Override
    public void update(float temp, float humidity, float presure) {
        System.out.println("StatisticsDisplay.update: " + temp + " " + humidity + " " + presure);
    }
}
