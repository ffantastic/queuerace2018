package io.openmessaging;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;


import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class Bucket {
    private String bucketName;
    private CacheManager cacheManager;
    private MessageWriter messageWriter;
    private MessageReader messageReader;
    private static final String DATA_ROOT_PATH = "/alidata1/race2018/data/";

    public Bucket(int number) throws IOException {
        bucketName = "bucket-" + number;
        System.out.println("Initializing Bucket " + bucketName);
        cacheManager = new CacheManager();
        messageWriter = new MessageWriter(DATA_ROOT_PATH + bucketName);
    }

    public void Put(String queueName, byte[] body) {
        Segment seg = this.cacheManager.GetCache(queueName);

        seg.Lock();
        try {
            boolean appendSuccess = seg.Append(queueName, body);
            if (!appendSuccess || seg.IsFull(Segment.DEFAULT_BODY_SIZE_BYTE)) {
                // need to flush to disk
                FlushCache(seg);
            }

            // try again after flushing
            if (!appendSuccess) {
                appendSuccess = seg.Append(queueName, body);
                if (!appendSuccess) {
                    throw new RuntimeException("Can't append message to queue " + queueName);
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            seg.Unlock();
        }
    }

    public int FlushExpiry(long expiryTimeSpanInMs) {
        int flushedNumber = 0;
        List<Segment> segs = cacheManager.GetCaches(false, expiryTimeSpanInMs);
        for (Segment seg : segs) {
            seg.Lock();
            try {
                if (seg.IsEmpty()) {
                    continue;
                }
                flushedNumber++;
                FlushCache(seg);
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                seg.Unlock();
            }
        }

        return flushedNumber;
    }

    public void ReleaseWriteResource() {
        List<Segment> segs = cacheManager.GetCaches(false, 0);
        for (Segment seg : segs) {
            seg.buffer = null;
        }

        this.messageWriter.CloseChannel();
        // System.gc();
    }

    private void FlushCache(Segment seg) {
        int globalStartOffset = this.messageWriter.Write(seg);
        seg.MergeIndex(globalStartOffset);
        seg.Reset();
    }

    public Collection<byte[]> Get(String queueName, long offset, int num) {
        Segment seg = this.cacheManager.GetCache(queueName);
        Object[] result = seg.Lookup(queueName, (int) offset, num);
        return this.messageReader.ReadMessage((int) result[0], (List<IndexChunk>) result[1], num);
    }

}
