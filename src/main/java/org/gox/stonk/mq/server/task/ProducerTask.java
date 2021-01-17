package org.gox.stonk.mq.server.task;

import org.gox.stonk.mq.message.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class ProducerTask implements StonkTask {

    private Socket clientSocket;
    private BufferedReader in;
    private PrintWriter out;
    private final BlockingQueue<Message> queue;

    public ProducerTask(Socket socket, BufferedReader in, BlockingQueue<Message> queue){
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
        try {
            Thread.currentThread().setName("producer_" + Thread.currentThread().getId());
            while (true) {
                String payload = in.readLine();
                String threadName = Thread.currentThread().getName();
                Message msg = new Message(threadName, payload);
                queue.put(msg);
                out.println("ACK");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getType() {
        return "producer";
    }
}
