package io.openmessaging;

import java.util.Collection;

public class ValidationChecker {
    public static void main(String... args) {
        QueueStore queueStore = null;

        try {
            Class queueStoreClass = Class.forName("io.openmessaging.DefaultQueueStoreImpl");
            queueStore = (QueueStore) queueStoreClass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }

        for (int i = 1000; i < 1100; i++) {
            queueStore.put("q1", String.valueOf(i).getBytes());
        }

        Collection<byte[]> result = queueStore.get("q1", 0, 10);
        for (byte[] r : result) {
            System.out.println(new String(r));
        }

    }
}
