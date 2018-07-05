package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.server.ExportException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Bucket {
    public static final int DEFAULT_BODY_SIZE_BYTE = 50 + 10;
    // average message number is about 2k/queue
    private static final int BUFFER_CAPCITY_BYTE = 87 * 1024 * 1024;
    private static final int FILE_SIZE = 800 * 1024 * 1024;
    private static final String DATA_ROOT_PATH = "/alidata1/race2018/data/";//"C:/Users/wenfan/Desktop/aliTest/";//

    private String bucketName;
    private int segmentNo = 0;
    private MessageWriter messageWriter;
    public final ConcurrentHashMap<String, MessageReader> ReaderMap = new ConcurrentHashMap<>();
    public ConcurrentHashMap<String, Index> indexTable = new ConcurrentHashMap<>();
    public String CurrentFileName;
    private final ReentrantLock lock = new ReentrantLock();
    public ByteBuffer buffer;
    private volatile boolean canWrite = true;
    private volatile int offset = 0;

    public Bucket(int number) throws IOException {
        bucketName = "bucket-" + number;
        System.out.println("Initializing Bucket " + bucketName);
        CurrentFileName = bucketName + "." + segmentNo;
        messageWriter = new MessageWriter(DATA_ROOT_PATH + CurrentFileName);
        ReaderMap.put(CurrentFileName, new MessageReader(DATA_ROOT_PATH + CurrentFileName));
        buffer = ByteBuffer.allocateDirect(BUFFER_CAPCITY_BYTE);
    }

    private void WaitUntilWritable() {
        try {
            while (!canWrite) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void Put(String queueName, byte[] body) {
        this.WaitUntilWritable();
        this.lock.lock();
        try {
            this.WaitUntilWritable();
            Index index = indexTable.get(queueName);
            if (index == null) {
                index = new Index(this);
                indexTable.put(queueName, index);
            }

            if (buffer.remaining() < body.length + 2) {
                FlushBuffer();
            }

            int offsetLocal = this.buffer.position();
            this.buffer.putShort((short) body.length);
            this.buffer.put(body);
            boolean needToFlushIndex = index.IndexMessage(offsetLocal + offset);
            if (needToFlushIndex) {
                FlushIndex(index);
            }

            int totalWrite = offset + this.buffer.position();
            if (totalWrite > FILE_SIZE) {
                this.RotateFile();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            this.lock.unlock();
        }
    }

    private void FlushIndex(Index index) {
        if (index.GetCurChunkSize() > this.buffer.remaining()) {
            FlushBuffer();
        }

        index.FlushCurrentChunk(this.buffer, offset + buffer.position());
    }

    public void FlushBuffer() {
        int bufferLength = this.buffer.position();
        offset += bufferLength;
        messageWriter.Write(this.buffer);
        this.buffer.clear();
    }

    private void RotateFile() {
        this.canWrite = false;
        this.FlushRemaining();
        this.CurrentFileName = this.bucketName + "." + (++segmentNo);
        try {
            this.messageWriter.OpenFile(DATA_ROOT_PATH + CurrentFileName);
            ReaderMap.put(CurrentFileName, new MessageReader(DATA_ROOT_PATH + CurrentFileName));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        offset = 0;
        canWrite = true;
    }

    public void FlushRemaining( ) {
        System.out.println(String.format("%s size is %d, flush remaining", this.CurrentFileName, this.offset / (1024 * 1024)));
        for (Map.Entry<String, Index> entry : this.indexTable.entrySet()) {
            Index index = entry.getValue();
            this.FlushIndex(index);
        }
        this.FlushBuffer();
        this.messageWriter.CloseChannel();
    }


    public Collection<byte[]> Get(String queueName, long offset, int num) {
        Index index = this.indexTable.get(queueName);
        List<IndexResult> result = index.Lookup((int) offset, num);
        Collection<byte[]> answer = new ArrayList<>();
        if (result == null || result.size() == 0) {
            return answer;
        }

        for (IndexResult r : result) {
            answer.addAll(this.ReaderMap.get(r.fileName).ReadMessage(r.offsets));
        }

        return answer;
    }

}
