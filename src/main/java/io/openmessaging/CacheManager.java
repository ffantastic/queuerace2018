package io.openmessaging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheManager {
    // try to seperate different queue messages to different message
    // if messages from same queue are actually clustered by time very well, the SEG_NUM can be 1
    private static final int SEG_NUM = 1;
    private final ConcurrentHashMap<Integer, Segment> cache = new ConcurrentHashMap<>();

    public CacheManager() {
        for (int i = 0; i < SEG_NUM; i++) {
            cache.put(i, new Segment());
        }
    }

    /**
     * lock free
     *
     * @param queueName
     * @return
     */
    public Segment GetCache(String queueName) {
        Segment seg = this.cache.get(GetSegmentIndex(queueName));
        return seg;
    }

    public List<Segment> GetCaches(boolean allowEmptyBuffer, long expriyTimeSpanInMs) {
        List<Segment> result = new ArrayList<>();
        for (int i = 0; i < SEG_NUM; i++) {
            Segment seg = cache.get(i);
            if ((allowEmptyBuffer || !seg.IsEmpty()) && System.currentTimeMillis() - seg.lastAppendTimestampInMs > expriyTimeSpanInMs) {
                result.add(seg);
            }
        }

        return result;
    }

    private Integer GetSegmentIndex(String queueName) {
        // to save calculate
        if (SEG_NUM == 1) {
            return 0;
        }

        return Math.abs(queueName.hashCode()) % SEG_NUM;
    }

}
