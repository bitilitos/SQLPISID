package ReceiveCloudMongo;//(c) ISCTE-IUL, Pedro Ramos, 2022


import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;

import java.io.*;
import java.util.*;
import java.util.List;
import java.sql.*;
import javax.swing.*;
import java.awt.event.*;
import java.awt.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class WriteMysql extends Thread {
    static JTextArea documentLabel = new JTextArea("\n");
    private BlockingQueue<String> messageQueue;
    static Connection connTo;
    static String sql_database_connection_to = "jdbc:mariadb://localhost:3306/rats";
    static String sql_database_password_to = "paloco2023";
    static String sql_database_user_to = "root";
    String sql_table_to = new String();


    public WriteMysql(String sql_table_to, BlockingQueue<String> messageQueue) {
        this.messageQueue = messageQueue;
        this.sql_table_to = sql_table_to;

    }

    public static void createWindow() {
        JFrame frame = new JFrame("Data Bridge - PC2 - mySQL");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JLabel textLabel = new JLabel("Data from MQTT: g7_tempRead, g7_movRead, g7_alert", SwingConstants.CENTER);
        textLabel.setPreferredSize(new Dimension(600, 30));
        JScrollPane scroll = new JScrollPane(documentLabel, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scroll.setPreferredSize(new Dimension(600, 200));
        JButton b1 = new JButton("Stop the program");
        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
        frame.getContentPane().add(scroll, BorderLayout.CENTER);
        frame.getContentPane().add(b1, BorderLayout.PAGE_END);
        frame.setLocationRelativeTo(null);
        frame.pack();
        frame.setVisible(true);
        b1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                System.exit(0);
            }
        });
    }

    @Override
    public void run() {

        connectoDatabase();
        while (true) {
            try {
                String message = messageQueue.take();
                documentLabel.insert(message + "\n", 0);
                writeToMySQL(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void connectoDatabase() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            connTo = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to, sql_database_password_to);
            documentLabel.append("SQl Connection:" + sql_database_connection_to + "\n");
            documentLabel.append("Connection To MariaDB Destination " + sql_database_connection_to + " Suceeded" + "\n");
        } catch (Exception e) {
            System.out.println("Mysql Server Destination down, unable to make the connection. " + e);
        }
    }


    //TENHO QUE ALTERAR ESTA FUNÇÃO PARA LIGAR COM SP E TRIGGERS

    public void writeToMySQL(String c) throws SQLException, InterruptedException {
        String sqlQuery = "";
        DBObject reading = getDBObjectFromReading(c);
        CallableStatement stmt = null;
        try {

            if (sql_table_to.equals("passagesmeasurements")) {
                String query = "{CALL spCreatePassagesMeasurements(?,?,?,?,?,?,?)}";
                stmt = connTo.prepareCall(query);
                stmt = statementForPassageMeasurementsSP(reading, stmt);
            } else if (sql_table_to.equals("temperaturemeasurements")) {
                String query = "{CALL spCreateTemperatureMeasurements(?,?,?,?,?,?,?)}";
                stmt = connTo.prepareCall(query);
                stmt = statementForTemperatureMeasurementsSP(reading, stmt);
            } else {
                String query = "{CALL spCreateAlert(?,?,?,?,?,?)}";
                stmt = connTo.prepareCall(query);
                stmt = statementForAlertsSP(reading, stmt);
            }
            stmt.executeUpdate();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database to table " + sql_table_to + "\n" + e);
            System.out.println(sqlQuery);
            if (!connTo.isValid(1)) {
                while (!connTo.isValid(2)) {
                    connectoDatabase();
                    System.out.println(sql_table_to + " Thread is trying to reconnect");
                }
                writeToMySQL(c);
            }

        } finally {
            if(stmt != null)
                stmt.close();
        }


    }



    private CallableStatement statementForPassageMeasurementsSP(DBObject reading, CallableStatement cs) throws SQLException {
        CallableStatement stmt = cs;

        stmt.setString(1, reading.get("_id").toString());
        stmt.setTimestamp(2, Timestamp.valueOf(reading.get("Hour").toString()));
        stmt.setInt(4, (Integer) reading.get("EntranceRoom"));
        stmt.setInt(3, (Integer) reading.get("ExitRoom"));
        stmt.setBoolean(5, (Boolean) (reading.get("isValid")));
        stmt.setString(6, (String) reading.get("Error"));
        stmt.setString(7, "");
        return stmt;
    }

    private CallableStatement statementForTemperatureMeasurementsSP(DBObject reading, CallableStatement cs) throws SQLException {
        CallableStatement stmt = cs;

        stmt.setString(1, reading.get("_id").toString());
        stmt.setTimestamp(2, Timestamp.valueOf(reading.get("Hour").toString()));
        stmt.setDouble(3, (Double) reading.get("Measure"));
        stmt.setInt(4, (Integer) reading.get("Sensor"));
        stmt.setBoolean(5, (Boolean) (reading.get("isValid")));
        stmt.setString(6, (String) reading.get("Error"));
        stmt.setString(7, "");
        return stmt;
    }

    private CallableStatement statementForAlertsSP(DBObject reading, CallableStatement cs) throws SQLException {
        CallableStatement stmt = cs;
        stmt.setTimestamp(1, Timestamp.valueOf(reading.get("Hour").toString()));

        if(reading.containsField("EntranceRoom")){
            int entRoom = (Integer) reading.get("EntranceRoom");
            int exitRoom = (Integer) reading.get("ExitRoom");
            String roomConcat = Integer.toString(entRoom) + Integer.toString(exitRoom);
            stmt.setInt(2, Integer.parseInt(roomConcat));
        } else stmt.setInt(2, -1);

        if(reading.containsField("Measure")){
            stmt.setInt(3, (Integer) reading.get("Sensor"));
            stmt.setDouble(4, (Double) reading.get("Measure"));
        }
        else {
            stmt.setInt(3, -1);
            stmt.setDouble(4, -272.15);
        }
        stmt.setString(5, (String) reading.get("AlertType"));
        stmt.setString(6, (String) (reading.get("Message")));

        return stmt;
    }

    private void insertSingleForTesting() throws SQLException {

        String sqlQuery = "";
        CallableStatement stmt = null;

        try {
            String query = "{CALL spCreateTemperatureMeasurements(?,?,?,?,?,?,?)}";
            stmt = connTo.prepareCall(query);

            stmt.setString(1, Integer.toString((int) (Math.random() * 1000)));
            stmt.setTimestamp(2, Timestamp.valueOf("2023-04-30 18:27:21.105147"));
            stmt.setDouble(3, Double.parseDouble("7.699999999999987"));
            stmt.setInt(4, 1);
            stmt.setBoolean(5, true);
            stmt.setString(6, "");
            stmt.setString(7, "");


            stmt.executeUpdate();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(sqlQuery);
        } finally {
            stmt.close();
        }

    }


    private DBObject getDBObjectFromReading (String reading) {
        try{
            DBObject document_json;
            document_json = (DBObject) JSON.parse(reading);
            return document_json;
        } catch (Exception e){
            System.out.println(e);
            return null;
        }

    }

    public static Connection getConnTo() {return  connTo;}

    public static void main(String[] args) {
        WriteMysql write2mysql = new WriteMysql("temperaturemeasurements", null);
        write2mysql.connectoDatabase();
        try {
            write2mysql.insertSingleForTesting();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}

