package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageWriter {

    private String filename;
    private int pos;
    private RandomAccessFile raf;
    private FileChannel outChannel;

    private static final int BUFFER_SIZE = 15 * 1024 * 1024;
    private List<ByteBuffer> buffer;
    private int size;

    private final Lock writeLock = new ReentrantLock();
    private static final Lock flushLock = new ReentrantLock();

    public MessageWriter(String fileName) throws IOException {
        this.filename = fileName;
        this.pos = 0;
        this.size = 0;
        this.buffer = new ArrayList<>();

        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        this.raf = new RandomAccessFile(file, "rw");
        outChannel = raf.getChannel();
    }

    public int Write(Segment seg) {
        int originalPos = pos;
        ByteBuffer byteBuffer = seg.buffer;

        writeLock.lock();
        try {
            byteBuffer.flip();
            pos += byteBuffer.limit();
            size += byteBuffer.limit();
            buffer.add(byteBuffer);
            if (size >= BUFFER_SIZE) {
                this.Flush();
            }
        } finally {
            writeLock.unlock();
        }
        return originalPos;
    }

    private void Flush() {
        flushLock.lock();
        try {
            for (ByteBuffer byteBuffer : buffer) {
                while (byteBuffer.hasRemaining()) {
                    this.outChannel.write(byteBuffer);
                }
            }
            //release buffer
            buffer = new ArrayList<>();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            flushLock.unlock();
        }
    }
}
