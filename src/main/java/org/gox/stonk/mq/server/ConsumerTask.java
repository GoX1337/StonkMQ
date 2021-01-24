package org.gox.stonk.mq.server;

import org.gox.stonk.mq.StonkMqServer;
import org.gox.stonk.mq.message.Message;
import org.gox.stonk.mq.queue.StonkQueue;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.Socket;

public class ConsumerTask extends StonkTask {

    public ConsumerTask(StonkMqServer stonkMqServer, Socket socket, BufferedReader in, StonkQueue queue){
       super("consumer", stonkMqServer, queue, socket, in);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("consumer_" + Thread.currentThread().getId());
        while(true){
            try {
                Message msg = this.queue.pull();
                out.println(msg);
                String ack = in.readLine();
                if(ack == null){
                    stop();
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
