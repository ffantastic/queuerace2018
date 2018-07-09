package io.openmessaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultQueueStoreImpl extends QueueStore {
    // 1 bucket = 1 file, including many queues with the same hashing value remaining with BUCKET_NUM
    private static final int BUCKET_NUM = 15;
    private final ConcurrentHashMap<Integer, Bucket> bucketMap = new ConcurrentHashMap<>();
    // private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

    private volatile boolean isAllCacheFlushed = false;
    private final ReentrantLock gate = new ReentrantLock();

    public DefaultQueueStoreImpl() throws IOException {
        System.out.printf("Initializing DefaultQueueStoreImpl");
        for (int i = 0; i < BUCKET_NUM; i++) {
            bucketMap.put(Integer.valueOf(i), new Bucket(i));
        }
    }

    public void put(String queueName, byte[] message) {

        Bucket bucket = this.GetBucket(queueName);
        bucket.Put(queueName, message);
    }

    public Collection<byte[]> get(String queueName, long offset, long num) {
        if (!isAllCacheFlushed) {
            gate.lock();
            try {
                if (!isAllCacheFlushed) {
//                    System.out.println("cleanup thread shutting down");
//                    threadPool.shutdownNow();
//                    threadPool.awaitTermination(3, TimeUnit.MINUTES);
//                    System.out.println("cleanup thread is closed");

                    FinishWriting();
                    isAllCacheFlushed = true;
                }
//            } catch (InterruptedException ex) {
//                throw new RuntimeException("cleaning up not finished in limited time");
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                gate.unlock();
            }
        }

        Bucket bucket = this.GetBucket(queueName);
        return bucket.Get(queueName, offset, (int) num);
    }

    private Bucket GetBucket(String queueName) {
        if (BUCKET_NUM == 1) {
            return bucketMap.get(Integer.valueOf(0));
        }
        return bucketMap.get(Math.abs(queueName.hashCode()) % BUCKET_NUM);
    }

    private void FinishWriting() {
        for (int i = 0; i < BUCKET_NUM; i++) {
            Bucket bucket = this.bucketMap.get(i);
            bucket.FlushRemaining();
        }
    }
}

