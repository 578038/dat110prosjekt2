package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;
import no.hvl.dat110.common.TODO;

public class TemperatureDevice {

	private static final int COUNT = 10;

	public static void main(String[] args) {

		// simulated / virtual temperature sensor
		TemperatureSensor sn = new TemperatureSensor();

		// TODO - start

		
		// create a client object and use it to

		// - connect to the broker
		Client client = new Client("Temp", Common.BROKERHOST, Common.BROKERPORT);
		client.connect();
		
		
		// - publish the temperature(s)
		for(int i = 0; i < COUNT; i++) {
			
			int temp = sn.read();
			
			client.publish(Common.TEMPTOPIC, "Temprature: " + temp);
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		
		// - disconnect from the broker
		client.disconnect();
		
		// TODO - end

		System.out.println("Temperature device stopping ... ");

		//throw new UnsupportedOperationException(TODO.method());

	}
}
