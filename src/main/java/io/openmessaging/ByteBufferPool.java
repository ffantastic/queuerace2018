package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class ByteBufferPool {
    // 20 MB for a pool
    private static final int POOL_SIZE = 2;
    private static final int BUF_SIZE = 2 * 1024;
    private Semaphore semaphore = new Semaphore(POOL_SIZE);
    private List<ByteBuffer> pool = new ArrayList<>(POOL_SIZE);

    public ByteBuffer Acquire() {
        try {
            semaphore.acquire();
            synchronized (pool) {
                if (pool.size() == 0) {
                    return ByteBuffer.allocateDirect(BUF_SIZE);
                } else {
                    return pool.remove(pool.size() - 1);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void Release(ByteBuffer buffer) {
        buffer.clear();
        synchronized (pool) {
            pool.add(buffer);
        }
        semaphore.release();
        if (pool.size() > POOL_SIZE) {
            System.out.printf("---------ByteBufferPool exceeding normal size of %d, current size %d.%n", POOL_SIZE, pool.size());
        }
    }
}
