package ReceiveCloudMongo;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;


import java.util.*;
import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.swing.*;

public class ReceiveCloud  {
    static String cloud_server = new String();

    static Map<String, String> topicsToTablesMap = new HashMap<>();



//Função para mapear cada tópico do MQTT para uma tabela do MySQL
    private static void setTopicToTablesMap() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("ReceiveCloud.ini"));
            String cloudTopic = p.getProperty("cloud_topic");
            String mySQLTables = p.getProperty("sql_tables");
            cloud_server = p.getProperty("cloud_server");
            if (cloudTopic.contains(",")){
                String[] cloudTopic_vector = cloudTopic.split(",");
                String[] mySQLTables_vector = mySQLTables.split(",");
                for (int i = 0; i < cloudTopic_vector.length; i++) {
                    topicsToTablesMap.put(cloudTopic_vector[i].trim(), mySQLTables_vector[i].trim());
                }
            } else {
                topicsToTablesMap.put(cloudTopic,mySQLTables);
            }
        } catch (Exception e) {
            System.out.println("Error reading ReceiveCloud.ini file " + e);
            JOptionPane.showMessageDialog(null, "The ReceiveCloud ini file wasn't found.", "Data Migration", JOptionPane.ERROR_MESSAGE);
        }
    }

    public static void main(String[] args) {
        //createWindow();
        setTopicToTablesMap();
        for (Map.Entry<String, String> collection : topicsToTablesMap.entrySet()){
            Runnable thread = new Runnable() {
                @Override
                public void run() {
                    BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
                    String key = collection.getKey();
                    String value = collection.getValue();
                    ReceiveTopic cloud2java = new ReceiveTopic(messageQueue, collection.getKey(), cloud_server, collection.getValue());
                    WriteMysql write2mysql = new WriteMysql(collection.getValue(), messageQueue);
                    cloud2java.start();
                //    write2mysql.start();
                }
            };
            thread.run();
        }
    }
}