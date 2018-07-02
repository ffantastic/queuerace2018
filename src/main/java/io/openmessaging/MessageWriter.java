package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageWriter {

    private String filename;
    private int pos;
    private RandomAccessFile raf;
    private FileChannel outChannel;

    private final Lock flushLock = new ReentrantLock();
    private static final Lock flushLockGlobal = new ReentrantLock();

    public MessageWriter(String fileName) throws IOException {
        this.filename = fileName;
        this.pos = 0;

        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        this.raf = new RandomAccessFile(file, "rw");
        outChannel = raf.getChannel();
    }

    public int Write(Segment seg) {
        flushLockGlobal.lock();
        int originalPos = pos;
        ByteBuffer byteBuffer = seg.buffer;
        try {
            byteBuffer.flip();
            pos += byteBuffer.limit();
            while (byteBuffer.hasRemaining()) {
                this.outChannel.write(byteBuffer);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            flushLockGlobal.unlock();
        }
        return originalPos;
    }

    public void CloseChannel() {
        System.out.println("close writer for " + this.filename);
        try {
            this.outChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
