package org.gox.stonk.mq.queue;

import org.gox.stonk.mq.message.Message;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;

public class StonkQueue extends ArrayBlockingQueue<Message> {

    private final static String filePath = "/tmp/data/Stonk";
    private File stateFile;
    private Cursor writeCursor;
    private Cursor messageCursor;

    public StonkQueue(int maxDepth) {
        super(maxDepth);
        File dirFile = new File(filePath);
        dirFile.mkdirs();
        try {
            initStateFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        startSegmentGarbageThread();
    }

    public void startSegmentGarbageThread(){
        new Thread(() -> {
            while(true){
                try {
                    Files.list(Paths.get(filePath))
                            .filter(file -> !Files.isDirectory(file))
                            .filter(this::isSegmentToGarbage)
                            .forEach(this::deleteFile);
                    Thread.sleep(3000);
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private boolean isSegmentToGarbage(Path path){
        String filename = path.getFileName().toString();
        if(!filename.startsWith("segment")){
            return false;
        }
        int segmentId = Integer.parseInt(filename.substring(7));
        return writeCursor.getSegmentId() > segmentId && messageCursor.getSegmentId() > segmentId;
    }

    private void deleteFile(Path path){
        try {
            Files.delete(path);
            System.out.println("Garbager deleted file: " + path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initStateFile() throws IOException {
        stateFile = new File(filePath + "/queue");
        if(!stateFile.exists()){
            createFile(stateFile);
        } else {
            loadStateCursors(stateFile);
        }
    }

    private void createFile(File file) throws IOException {
        if(file.createNewFile()) {
            writeCursor = new Cursor();
            messageCursor = new Cursor();
            saveStateFile();
        }
    }

    private void loadStateCursors(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String currentLine = reader.readLine();
        reader.close();
        String[] cursors = currentLine.split(";");
        String[] writeCursorElems = cursors[0].split(":");
        String[] messageCursorElems = cursors[1].split(":");
        writeCursor = new Cursor(writeCursorElems[0], Integer.parseInt(writeCursorElems[1]));
        messageCursor = new Cursor(messageCursorElems[0], Integer.parseInt(messageCursorElems[1]));
        System.out.println("Loaded write cursor: " + writeCursor + " and message cursor:" + messageCursor);
    }

    private void saveStateFile(){
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(stateFile));
            writer.write(writeCursor + ";" + messageCursor + "\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createNewSegment(){
        File file = new File(filePath + "/" + writeCursor.getSegmentName());
        if(file.length() / 1024 >= 4){
            writeCursor.addSegment();
            writeCursor.setOffset(0);
        }
    }

    public void push(Message message){
        try {
            createNewSegment();
            RandomAccessFile writer = new RandomAccessFile(filePath + "/" + writeCursor.getSegmentName(), "rw");
            byte[] msgBuffer = message.serialize();
            writer.seek(writeCursor.getOffset());
            writer.write(msgBuffer);
            writer.close();
            writeCursor.addOffset(msgBuffer.length);
            saveStateFile();
            super.put(message);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Message pull(){
        Message msg = null;
        try {
            RandomAccessFile reader = new RandomAccessFile(filePath + "/" + messageCursor.getSegmentName(), "r");
            reader.seek(messageCursor.getOffset());
            String msgStr = reader.readLine();
            reader.close();
            if(msgStr == null){
                messageCursor.addSegment();
                messageCursor.setOffset(0);
            } else {
                messageCursor.addOffset(msgStr.getBytes(StandardCharsets.UTF_8).length + 1);
            }
            msg = super.take();
            saveStateFile();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public Message takeOnFs(){
        try {
            RandomAccessFile reader = new RandomAccessFile(filePath + "/" + messageCursor.getSegmentName(), "r");
            reader.seek(messageCursor.getOffset());
            String msgStr = reader.readLine();
            reader.close();
            if(msgStr == null){
                messageCursor.addSegment();
                messageCursor.setOffset(0);
                return takeOnFs();
            } else {
                messageCursor.addOffset(msgStr.getBytes(StandardCharsets.UTF_8).length + 1);
                return Message.deserialize(msgStr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws InterruptedException {
        StonkQueue queue = new StonkQueue(10000);
        new Thread(() -> {
            while(true){
                try {
                    Thread.sleep(1500);
                    System.out.println(queue.pull());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        int i = 0;
        while(true){
            queue.push(new Message("kek " + i, "hello there " + i));
            System.out.println("message pushed " + i);
            i++;
            Thread.sleep(1000);
        }
    }
}
