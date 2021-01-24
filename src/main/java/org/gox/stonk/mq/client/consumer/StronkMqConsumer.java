package org.gox.stonk.mq.client.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.time.Instant;

public class StronkMqConsumer {

    private Socket clientSocket;
    private BufferedReader in;
    private PrintWriter out;
    private String ip;
    private int port;
    private long lastConnectionTimestamp;

    public StronkMqConsumer(String ip, int port) throws IOException {
        this.ip = ip;
        this.port = port;
        connect();
    }

    private void connect() throws IOException {
        clientSocket = new Socket(ip, port);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println("consumer");
        lastConnectionTimestamp = System.currentTimeMillis();
        System.out.println("Consumer connected");
    }

    private void reconnect() throws IOException {
        if((System.currentTimeMillis() - lastConnectionTimestamp) > 1000){
            try {
                System.out.println("Connection server lost. Reconnecting...");
                connect();
            } catch (ConnectException e){
            } finally {
                lastConnectionTimestamp = System.currentTimeMillis();
            }
        }
    }

    private void listen() throws IOException, InterruptedException {
        while (true) {
            String message = in.readLine();
            if(message == null){
               reconnect();
            } else {
                System.out.println(Instant.now() + ": " + message);
                out.println("READ_ACK");
            }
        }
    }

    public static void main(String... args){
        try {
            StronkMqConsumer consumer = new StronkMqConsumer("127.0.0.1", 6666);
            consumer.listen();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
