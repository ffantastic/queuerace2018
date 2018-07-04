package io.openmessaging;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Segment {
    public static final int DEFAULT_BODY_SIZE_BYTE = 50 + 10;
    // average message number is about 2k/queue
    private static final int BUFFER_CAPCITY_BYTE = 1024 * 1024;
    private final ReentrantLock lock = new ReentrantLock();

    public ConcurrentHashMap<String, Index> subIndexTable;
    public ByteBuffer buffer;
    private String tailQueueName = null;
    public long lastAppendTimestampInMs;

    // the perf can be roughly measured by percentage of continousMessageCount / messageCount, from 0% ~ 100%
    volatile private int messageCount = 0;
    volatile private int continousMessageCount = 0;

    public Segment() {
        subIndexTable = new ConcurrentHashMap<>();
        buffer = ByteBuffer.allocateDirect(BUFFER_CAPCITY_BYTE);
    }

    public void Lock() {
        this.lock.lock();
    }

    public void Unlock() {
        this.lock.unlock();
    }

    public boolean IsFull(int remain) {
        return this.buffer.remaining() < remain;
    }

    public boolean IsEmpty() {
        return this.buffer.position() == 0;
    }

    public boolean Append(String queueName, byte[] body) {

        if (buffer.remaining() >= body.length + 2) {
            lastAppendTimestampInMs = System.currentTimeMillis();
            messageCount++;
            int currentLocalOffset = buffer.position();
            buffer.putShort((short) body.length);
            buffer.put(body);

            // update local index
            Index index = subIndexTable.get(queueName);
            if (index == null) {
                index = new Index();
                subIndexTable.put(queueName, index);
            }

            if (tailQueueName == null || !tailQueueName.equals(queueName)) {
                tailQueueName = queueName;
                index.AddNewChunk(currentLocalOffset);
            } else {
                continousMessageCount++;
                index.UpdateChunk();
            }

            return true;
        }

        return false;
    }

    public void Reset() {
        buffer.clear();
        tailQueueName = null;
    }

    public void MergeIndex(int globalStartOffset) {
        for (Map.Entry<String, Index> entry : subIndexTable.entrySet()) {
            entry.getValue().Merge(globalStartOffset);
        }
    }

    public Object[] Lookup(String queueName, int offset, int num) {
        Index index = this.subIndexTable.get(queueName);
        if (index == null) {
            throw new IllegalArgumentException("Queuename doesn't exist in local index table , queue: " + queueName);
        }

        return index.Lookup(offset, num);

    }
}
