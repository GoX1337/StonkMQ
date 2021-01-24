package org.gox.stonk.mq.message;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Message {

    private String producerName;
    private LocalDateTime timestamp;
    private String payload;

    public Message(String producerName, String payload) {
        this.producerName = producerName;
        this.timestamp = LocalDateTime.now();
        this.payload = payload;
    }

    public Message(String producerName, LocalDateTime timestamp, String payload) {
        this.producerName = producerName;
        this.timestamp = timestamp;
        this.payload = payload;
    }

    public byte[] serialize(){
        return (timestamp + ";" + producerName + ";" + payload + "\n").getBytes(StandardCharsets.UTF_8);
    }

    public static Message deserialize(String str){
        String[] msgElems = str.split(";");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        LocalDateTime dateTime = LocalDateTime.parse(msgElems[0], formatter);
        return new Message(msgElems[1], dateTime, msgElems[2]);
    }

    @Override
    public String toString() {
        return "{" +
                "producerName='" + producerName + '\'' +
                ", timestamp=" + timestamp +
                ", payload='" + payload + '\'' +
                '}';
    }
}
