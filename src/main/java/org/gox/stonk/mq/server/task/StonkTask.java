package org.gox.stonk.mq.server.task;

public interface StonkTask extends Runnable {

    String getType();
}
