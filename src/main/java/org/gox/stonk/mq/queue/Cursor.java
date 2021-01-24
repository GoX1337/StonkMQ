package org.gox.stonk.mq.queue;

public class Cursor {

    private String segmentName;
    private int segmentId;
    private int offset;
    private final static String SEGMENT = "segment";

    public Cursor(String segmentName, int offset) {
        this.segmentName = segmentName;
        this.segmentId = Integer.parseInt(segmentName.substring(SEGMENT.length()));
        this.offset = offset;
    }

    public Cursor() {
        this.segmentId = 0;
        this.offset = 0;
        this.segmentName = SEGMENT + segmentId;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void addOffset(int shift) {
        this.offset += shift;
    }

    public void addSegment(){
        this.segmentId++;
        this.segmentName = SEGMENT + this.segmentId;
    }

    public String toString(){
        return segmentName + ":" + offset;
    }
}
