package designPattern.observer;

/**
 * @author bystander
 * @date 2021/2/19
 */
public class WeatherStation {

    public static void main(String[] args) {
        WeatherData weatherData = new WeatherData();

        StatisticsDisplay statisticsDisplay = new StatisticsDisplay(weatherData);
        CurrentConditionsDisplay currentConditionsDisplay = new CurrentConditionsDisplay(weatherData);

        weatherData.setMeasurements(0, 0, 0);

        weatherData.setMeasurements(1, 1, 1);
    }
}
