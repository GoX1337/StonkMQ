package org.gox.stonk.mq.server;

import org.gox.stonk.mq.StonkMqServer;
import org.gox.stonk.mq.message.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class ProducerTask extends StonkTask {

    public ProducerTask(StonkMqServer stonkMqServer, Socket socket, BufferedReader in, BlockingQueue<Message> queue){
        super("producer", stonkMqServer, queue, socket, in);
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName("producer_" + Thread.currentThread().getId());
            while (true) {
                String payload = in.readLine();
                if(payload == null){
                    stop();
                    break;
                }
                String threadName = Thread.currentThread().getName();
                Message msg = new Message(threadName, payload);
                queue.put(msg);
                out.println("PUT_ACK");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
