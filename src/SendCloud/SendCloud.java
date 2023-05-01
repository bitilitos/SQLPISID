package SendCloud;

import SendCloud.*;
import ReceiveCloudMongo.*;
import org.eclipse.paho.client.mqttv3.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;



public class SendCloud extends Thread implements MqttCallback  {
	private static MqttClient mqttclient;
	private static String cloud_server = new String();
    private static final String cloud_topic = "g7_experiment";
	private String mongo_collections = new String();
	private int experimentID = -1;

	public SendCloud() {
		connectCloud();
	}

	public void publishSensor(String leitura) {
		try {
			MqttMessage mqtt_message = new MqttMessage();
			mqtt_message.setPayload(leitura.getBytes());
			mqttclient.publish(cloud_topic, mqtt_message);
			System.out.println("Published topic " + cloud_topic + " : message:" + mqtt_message);
		} catch (MqttException e) {
			e.printStackTrace();}
	}


	@Override
	public void run() {
		while (true) {
			try {
				sleep(30000);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			if (mqttclient.isConnected()) {
				System.out.println("Cloud connected is " + mqttclient.isConnected() + " on topic " + cloud_topic);
				String query = "";
				String message = "";

				if (experimentID == -1) {
					query = "SELECT * FROM isExperimentToStart";
					message = "START_EXPERIMENT";
				}else {
					query = "SELECT * FROM experiment WHERE experiment.IDExperiment = " + experimentID +
							" AND experiment.IsActive = 0";
					message = "STOP_EXPERIMENT";
				}

				try {
					Statement stmt = WriteMysql.getConnTo().createStatement();
					ResultSet rs = stmt.executeQuery(query);
					if (rs.next()) {

						if (experimentID == -1)
							startExperiment(rs.getInt("IDExperiment"));

						else
							experimentID = -1;

						publishSensor(message);
						System.out.println(message + " Topic: "  + cloud_topic + " published");
					}

				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			else {
				System.out.println("MQTT on topic " + cloud_topic + " is not connected");

				try {
					sleep(1000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

		}
	}

	private void startExperiment(int id) {
		experimentID = id;
		try {
			String query = "{CALL spStartExperiment(?)}";
			CallableStatement stmt = WriteMysql.getConnTo().prepareCall(query);
			stmt.setInt(1, experimentID);
			stmt.executeUpdate();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}


	}


	public void connectCloud() {
        try {
			Properties p = new Properties();
			p.load(new FileInputStream("SendCloud.ini"));
			cloud_server = p.getProperty("cloud_server");
            mqttclient = new MqttClient(cloud_server, "SimulateSensor" + cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
			System.out.println("Cloud connected is " + mqttclient.isConnected() + " on topic " + cloud_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public void connectionLost(Throwable cause) {
		System.out.println("Connection lost");
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		System.out.println("Delivery complete\n" );
		System.out.println("Message sent: " + token.getMessageId() + "\n");
	}

	@Override
	public void messageArrived(String topic, MqttMessage message){ }

}
