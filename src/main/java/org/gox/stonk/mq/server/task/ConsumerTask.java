package org.gox.stonk.mq.server.task;

import org.gox.stonk.mq.message.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class ConsumerTask implements StonkTask {

    private Socket clientSocket;
    private BufferedReader in;
    private PrintWriter out;
    private final BlockingQueue<Message> queue;

    public ConsumerTask(Socket socket, BufferedReader in, BlockingQueue<Message> queue){
        this.queue = queue;
        this.clientSocket = socket;
        try {
            this.in = in;
            this.out = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName("consumer_" + Thread.currentThread().getId());
        while(true){
            try {
                Message msg = this.queue.take();
                out.println(msg);
                String ack = in.readLine();
                if(ack == null){
                    System.out.println("Consumer disconnected");
                    break;
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String getType() {
        return "consumer";
    }
}
