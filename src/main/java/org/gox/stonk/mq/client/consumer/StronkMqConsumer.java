package org.gox.stonk.mq.client.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.Instant;

public class StronkMqConsumer {

    private Socket clientSocket;
    private BufferedReader in;
    private PrintWriter out;

    public StronkMqConsumer(String ip, int port) throws IOException {
        clientSocket = new Socket(ip, port);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println("consumer");
        startHeartbeatTread();
    }

    private void startHeartbeatTread() {
        new Thread(() -> {
            out.println("heartbeat");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ).start();
    }

    private void listen() throws IOException {
        String message = "";
        while ((message = in.readLine()) != null) {
            System.out.println(Instant.now() + ": " + message);
        }
    }

    public static void main(String... args){
        StronkMqConsumer consumer;
        try {
            consumer = new StronkMqConsumer("127.0.0.1", 6666);
            System.out.println("Consumer connected");
            consumer.listen();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
