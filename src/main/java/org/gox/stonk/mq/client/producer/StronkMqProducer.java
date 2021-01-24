package org.gox.stonk.mq.client.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class StronkMqProducer {

    private Socket clientSocket;
    private BufferedReader in;
    private PrintWriter out;

    public StronkMqProducer(String ip, int port) throws IOException {
        clientSocket = new Socket(ip, port);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println("producer");
    }

    public boolean sendMessage(String msg) throws IOException, InterruptedException {
        out.println(msg);
        String response = in.readLine();
        if(response == null){
            throw new InterruptedException("Connection server lost");
        }
        return "PUT_ACK".equals(response);
    }

    public static void main(String... args){
        StronkMqProducer producer;
        try {
            producer = new StronkMqProducer("127.0.0.1", 6666);
            System.out.println("Producer connected");

            int i = 1;
            while(true) {
                producer.sendMessage("hello server " + i);
                System.out.println("Message sent " + i++);
                Thread.sleep(500);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
