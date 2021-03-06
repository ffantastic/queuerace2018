package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

public class MessageReader {
    private String fileName;
    private RandomAccessFile raf;
    private FileChannel inChannel;
    private final ReentrantLock readlock = new ReentrantLock();
    private final ReentrantLock indexReadLock = new ReentrantLock();
    private final ByteBufferPool bufferPool = new ByteBufferPool();
    // private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(2 * 1024);
    // private final ByteBuffer indexReadBuffer = ByteBuffer.allocateDirect(2 * 1024);

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
    }

    public Collection<byte[]> ReadMessage(int[] offsets) {
        if (this.inChannel == null) {
            readlock.lock();
            try {
                if (this.inChannel == null) {
                    this.openFile();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                readlock.unlock();
            }
        }

        ByteBuffer readBuffer = bufferPool.Acquire();
        try {
            List<byte[]> result = new ArrayList<>(offsets.length);
            for (int i = 0; i < offsets.length; i++) {
                this.inChannel.read(readBuffer, offsets[i]);
                readBuffer.flip();
                int len = readBuffer.getShort();
                if (len > readBuffer.capacity()) {
                    throw new RuntimeException("MessageReader, message length exceed buffer length " + len);
                }
                byte[] data = new byte[len];
                readBuffer.get(data);
                readBuffer.clear();
                result.add(data);
            }
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            bufferPool.Release(readBuffer);
        }
    }

    public int[] ReadIndex(int indexOffset) {
        if (this.inChannel == null) {
            readlock.lock();
            try {
                if (this.inChannel == null) {
                    this.openFile();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                readlock.unlock();
            }
        }

        ByteBuffer indexReadBuffer = bufferPool.Acquire();
        try {
            this.inChannel.read(indexReadBuffer, indexOffset);
            indexReadBuffer.flip();
            int len = indexReadBuffer.getShort();
            if (len > indexReadBuffer.capacity()) {
                throw new RuntimeException("MessageReader, message length exceed buffer length " + len);
            }
            int[] index = new int[len];
            for (int i = 0; i < len; i++) {
                index[i] = indexReadBuffer.getInt();
            }
            indexReadBuffer.clear();
            return index;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            bufferPool.Release(indexReadBuffer);
        }
    }
}
