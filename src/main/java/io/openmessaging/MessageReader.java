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
            System.out.println(errMsg);
        }

        try {
            this.buf = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error mmap file:" + e.getMessage());
        }
    }

    public Collection<byte[]> ReadMessage(int[] offsets) {
        readlock.lock();
        try {
            if (this.buf == null) {
                this.openFile();
            }

            List<byte[]> result = new ArrayList<>(offsets.length);

            for (int i = 0; i < offsets.length; i++) {
                this.buf.position(offsets[i]);
                int len = this.buf.getShort();
                byte[] data = new byte[len];
                this.buf.get(data);
                result.add(data);
            }

            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            readlock.unlock();
        }

        return null;
    }

    public int[] ReadIndex(int indexOffset) {
        readlock.lock();
        try {
            if (this.buf == null) {
                this.openFile();
            }
            this.buf.position(indexOffset);
            int len = this.buf.getShort();
            int[] index = new int[len];
            for (int i = 0; i < len; i++) {
                index[i] = this.buf.getInt();
            }
            return index;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            readlock.unlock();
        }

        return null;
    }
}
