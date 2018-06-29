package io.openmessaging;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;


import java.io.IOException;
import java.util.Collection;

public class Bucket {
    private String bucketName;
    private WriteCacheManager writeCacheManager;
    private MessageWriter messageWriter;
    private MessageReader messageReader;
    private Index index;
    private static final String DATA_ROOT_PATH = "/alidata1/race2018/data/";

    public Bucket(int number) throws IOException {
        bucketName = "bucket-" + number;
        writeCacheManager = new WriteCacheManager();
        messageWriter = new MessageWriter(DATA_ROOT_PATH + bucketName);
        index = new Index();
    }

    public void Put(String queueName, byte[] body) {
        Segment seg = this.writeCacheManager.GetCache(queueName);

        seg.Lock();
        try {
            boolean appendSuccess = seg.Append(queueName, body);
            if(!appendSuccess || seg.IsFull(Segment.DEFAULT_BODY_SIZE_BYTE)){
                // need to flush to disk
            }


        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            seg.Unlock();
        }

        if (seg.IsFull(body.length)) {
            this.FlushCache(queueName, seg);
        }
        seg.Append(body);

        if (seg.IsFull(Segment.DEFAULT_BODY_SIZE_BYTE)) {
            this.FlushCache(queueName, seg);
        }

    }

    public Collection<byte[]> Get(String queueName, long offset, long num) {
        throw new NotImplementedException();
    }

    private void FlushCache(String queueName, Segment seg) {
        int length = seg.GetMessageLength();
        int offsetStart = messageWriter.Write(seg);
        index.Update(queueName, offsetStart, length, seg.GetMessageNumber());
        seg.Clear();
    }
}
