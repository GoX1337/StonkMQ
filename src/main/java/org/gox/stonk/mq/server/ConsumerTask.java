package org.gox.stonk.mq.server;

import org.gox.stonk.mq.StonkMqServer;
import org.gox.stonk.mq.message.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class ConsumerTask extends StonkTask {

    public ConsumerTask(StonkMqServer stonkMqServer, Socket socket, BufferedReader in, BlockingQueue<Message> queue){
       super("consumer", stonkMqServer, queue, socket, in);
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
                    stop();
                    break;
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }
}
