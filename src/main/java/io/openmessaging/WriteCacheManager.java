package io.openmessaging;

import java.util.concurrent.ConcurrentHashMap;

public class WriteCacheManager {
    private static final int SEG_NUM = 11;
    private final ConcurrentHashMap<Integer, Segment> cache = new ConcurrentHashMap<>();

    public WriteCacheManager() {
        for (int i = 0; i < SEG_NUM; i++) {
            cache.put(i, new Segment());
        }
    }

    /**
     * lock free
     * @param queueName
     * @return
     */
    public Segment GetCache(String queueName) {
        Segment seg = this.cache.get(GetSegmentIndex(queueName));
        return seg;
    }

    private Integer GetSegmentIndex(String queueName) {
        return Math.abs(queueName.hashCode()) % SEG_NUM;
    }
}
