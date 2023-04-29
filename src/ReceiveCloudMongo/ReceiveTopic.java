package ReceiveCloudMongo;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;


import java.util.*;
import javax.swing.*;
import java.util.concurrent.BlockingQueue;

public class ReceiveTopic extends Thread implements MqttCallback {
    private BlockingQueue<String> messageQueue;
    private MqttClient mqttclient;
    private static String cloud_server = new String();
    private String cloud_topic = new String();
    private String sql_table = new String();


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
            messageQueue.add(c.toString());
           // System.out.println(c);
        } catch (Exception e) {
            System.out.println(e);
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