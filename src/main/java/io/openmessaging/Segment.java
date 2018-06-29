package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Segment {
    public static final int DEFAULT_BODY_SIZE_BYTE = 50 + 10;
    // average message number is about 2k/queue
    private static final int BUFFER_CAPCITY_BYTE = 200 * 50;
    private final ReentrantLock lock = new ReentrantLock();

    public ConcurrentHashMap<String, List<IndexChunk>> subIndex;
    public ByteBuffer buffer;
    private String tailQueueName = null;

    // the perf can be roughly measured by percentage of continousMessageCount / messageCount, from 0% ~ 100%
    volatile private int messageCount = 0;
    volatile private int continousMessageCount = 0;

    public Segment() {
        subIndex = new ConcurrentHashMap<>();
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

    public boolean Append(String queueName, byte[] body) {

        if (buffer.remaining() >= body.length) {
            messageCount++;
            int currentLocalOffset = buffer.position();
            buffer.put(body);

            // update local index
            List<IndexChunk> indexChunkList = subIndex.get(queueName);
            if (indexChunkList == null) {
                subIndex.put(queueName, indexChunkList);
            }

            if (tailQueueName == null || !tailQueueName.equals(queueName)) {
                tailQueueName = queueName;
                indexChunkList.add(new IndexChunk(currentLocalOffset, body.length, 1));
            } else {
                continousMessageCount++;
                IndexChunk lastChunk = indexChunkList.get(indexChunkList.size() - 1);
                lastChunk.MessageNumber++;
                lastChunk.Length += body.length;
            }

            return true;
        }

        return false;
    }

    public void Reset() {
        buffer.clear();
        subIndex = new ConcurrentHashMap<>();
        tailQueueName = null;
    }
}
