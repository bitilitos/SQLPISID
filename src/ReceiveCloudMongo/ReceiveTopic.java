package ReceiveCloudMongo;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;


import java.io.*;
import java.sql.*;
import java.util.*;
import javax.swing.*;
import java.util.concurrent.BlockingQueue;

public class ReceiveTopic extends Thread implements MqttCallback {
    private BlockingQueue<String> messageQueue;
    private MqttClient mqttclient;
    private static String cloud_server = new String();
    private String cloud_topic = new String();
    private String sql_table = new String();

    private static FileWriter fw;
    private static File csvFile;


    public ReceiveTopic(BlockingQueue<String> messageQueue, String cloudTopic, String cloudServer, String sqlTable) {
        this.sql_table = sqlTable;
        this.cloud_server = cloudServer;
        this.cloud_topic = cloudTopic;
        this.messageQueue = messageQueue;
    }
    @Override
    public void run() {
        connecCloud();
    }
    public void connecCloud() {
        int i;
        getReadyCSV();
        try {
            i = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "ReceiveCloud"+String.valueOf(i)+"_"+cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage c)
            throws Exception {
        try {
            fw = new FileWriter(csvFile.getPath(), true);
            fw.append(c.toString());
            messageQueue.add(c.toString());
            System.out.println("Received: " + c.toString());
        } catch (Exception e) {
            System.out.println(e);
        }finally {
            fw.close();
        }
    }

    private static void getReadyCSV() {
        try {
            csvFile = new File("receivedMessages.csv");
            fw = new FileWriter(csvFile.getPath(), false);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("MQTT Connection Lost, cause:" + cause);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}