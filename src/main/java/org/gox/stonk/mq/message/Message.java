package org.gox.stonk.mq.message;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Message implements Serializable {

    private String producerName;
    private LocalDateTime timestamp;
    private String payload;

    public Message(String producerName, String payload) {
        this.producerName = producerName;
        this.timestamp = LocalDateTime.now();
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Message {" +
                "producerName='" + producerName + '\'' +
                ", timestamp=" + timestamp +
                ", payload='" + payload + '\'' +
                '}';
    }
}
