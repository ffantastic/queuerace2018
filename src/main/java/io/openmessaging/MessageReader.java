package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class MessageReader {
    private String fileName;
    private RandomAccessFile raf;
    private FileChannel inChannel;
    private ByteBuffer buf;
    private final ReentrantLock readlock = new ReentrantLock();
    private final ReentrantLock readlockGlobal = new ReentrantLock();

    public MessageReader(String fileName) {
        this.fileName = fileName;
    }

    private void openFile() {
        System.out.println("Initializing MessageReader for " + fileName);
        try {
            this.raf = new RandomAccessFile(this.fileName, "r");
            this.inChannel = raf.getChannel();
        } catch (FileNotFoundException e) {
            String errMsg = "Failed in openTopicFile. File " + fileName + " doesnot exist"
                    + e.getMessage();
        }

        try {
            this.buf = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error mmap file:" + e.getMessage());
        }
    }

    public Collection<byte[]> ReadMessage(int messageSkippedInFirstChunk, List<IndexChunk> index, int limit) {
        readlock.lock();
        try {
            if (this.buf == null) {
                this.openFile();
            }

            Collection<byte[]> result = new ArrayList<>(limit);
            int msgRead = 0;
            for (int i = 0; i < index.size(); i++) {
                int start = i == 0 ? messageSkippedInFirstChunk : 0;
                int end = index.get(i).MessageNumber - 1;
                if (i == index.size() - 1) {
                    int limitInLastChunk = limit - msgRead;
                    end = Math.min(end, start + limitInLastChunk - 1);
                }

                this.ReadChunk(index.get(i), start, end, result);
                msgRead = end - start + 1;
            }

            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            readlock.unlock();
        }

        return null;
    }

    private void ReadChunk(IndexChunk indexChunk, int start, int end, Collection<byte[]> result) {
        if (start > end || (end - start + 1) > indexChunk.MessageNumber) {
            throw new IllegalArgumentException(String.format("ReadChunk start , end, total [%d,%d,%d]", start, end, indexChunk.MessageNumber));
        }

        this.buf.position(indexChunk.Offset);
        int index = -1;
        while (this.buf.hasRemaining()) {
            index++;
            short messageLen = this.buf.getShort();
            if (index < start) {
                this.buf.position(this.buf.position() + messageLen);
                continue;
            }

            if (index > end) {
                break;
            }

            byte[] msg = new byte[messageLen];
            this.buf.get(msg);
            result.add(msg);
        }
    }
}
