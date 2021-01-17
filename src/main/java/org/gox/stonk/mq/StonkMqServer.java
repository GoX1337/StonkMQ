package org.gox.stonk.mq;

import org.gox.stonk.mq.message.Message;
import org.gox.stonk.mq.server.task.ConsumerTask;
import org.gox.stonk.mq.server.task.ProducerTask;
import org.gox.stonk.mq.server.task.StonkTask;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

public class StonkMqServer {

    private ServerSocket serverSocket;
    private ExecutorService executor;
    private BlockingQueue<Message> queue;
    private int nbConsumer = 0;
    private int nbProducer = 0;

    public StonkMqServer(int port, int maxNbThread, int maxMessageDepth){
        try {
            queue = new ArrayBlockingQueue<>(maxMessageDepth);
            executor = Executors.newFixedThreadPool(maxNbThread);
            serverSocket = new ServerSocket(port);
            System.out.println("Stonk MQ server listening on port " + port);
            startMonitorThread();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startMonitorThread(){
        Runnable task = () -> {
            int queueSize = -1;
            while(true) {
                if(queueSize != queue.size()){
                    queueSize = queue.size();
                    System.out.println("Queue size " + queue.size() + ((queue.remainingCapacity() == 0) ? " (it's full)": ""));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        new Thread(task).start();
    }

    public StonkTask createNewTask(Socket socketClient) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socketClient.getInputStream()));
        switch (in.readLine()){
            case "producer":
                nbProducer++;
                return new ProducerTask(socketClient, in, queue);
            case "consumer":
                nbConsumer++;
                return new ConsumerTask(socketClient, in, queue);
            default: return null;
        }
    }

    public void start() {
        try {
            while (true) {
                Socket socketClient = serverSocket.accept();
                StonkTask task = createNewTask(socketClient);
                System.out.println("New " + task.getType() + " connected (" + nbProducer + " producers; " + nbConsumer + " consumers)");
                executor.submit(task);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String... args) {
        System.out.println("Starting Stonk MQ server...");
        StonkMqServer stonkMqServer = new StonkMqServer(6666, 10, 10);
        stonkMqServer.start();
    }
}