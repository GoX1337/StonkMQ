package org.gox.stonk.mq.server;

import org.gox.stonk.mq.StonkMqServer;
import org.gox.stonk.mq.message.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public abstract class StonkTask implements Runnable {

    protected String type;
    private Socket clientSocket;
    protected BufferedReader in;
    protected PrintWriter out;
    protected final BlockingQueue<Message> queue;
    private final StonkMqServer stonkMqServer;

    public StonkTask(String type, StonkMqServer stonkMqServer, BlockingQueue queue, Socket clientSocket, BufferedReader in) {
        this.type = type;
        this.stonkMqServer = stonkMqServer;
        this.queue = queue;
        this.clientSocket = clientSocket;
        this.in = in;
        try {
            this.out = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop(){
        try {
            in.close();
            out.close();
            clientSocket.close();
            stonkMqServer.disconnect(type);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getType(){
        return type;
    }
}
