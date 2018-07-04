package io.openmessaging;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Index {

    private final List<Long> chunkList = new ArrayList<>();
    private int mergedPos = -1;


    public void AddNewChunk(int currentLocalOffset) {
        chunkList.add(IndexChunk.GetChunk(currentLocalOffset, 1));
    }

    public void UpdateChunk() {
        Long lastChunk = chunkList.get(chunkList.size() - 1);
        chunkList.set(chunkList.size() - 1, IndexChunk.AddMessage(lastChunk, 1));
    }

    public void Merge(int globalStartOffset) {
        while (mergedPos + 1 < chunkList.size()) {
            mergedPos++;
            long chunk = chunkList.get(mergedPos);
            chunkList.set(mergedPos, IndexChunk.AddOffset(chunk, globalStartOffset));
        }
    }

    public Object[] Lookup(int start, int num) {
        List<IndexChunk> result = new ArrayList<>();
        int curMessageIndex = -1;
        int messageSkippedInFisrtChunk = 0;

        int i = 0;
        for (; i < chunkList.size(); i++) {
            int messageNumber = IndexChunk.GetMessageNumber(chunkList.get(i));
            curMessageIndex += messageNumber;
            if (curMessageIndex >= start) {
                messageSkippedInFisrtChunk = messageNumber - curMessageIndex + start - 1;
                break;
            }
        }

        if (i == chunkList.size()) {
            // not exist
            return null;
        }

        result.add(new IndexChunk(chunkList.get(i)));
        int readNum = IndexChunk.GetMessageNumber(chunkList.get(i)) - messageSkippedInFisrtChunk;
        i++;
        for (; i < chunkList.size(); i++) {
            if (readNum >= num) {
                break;
            }

            result.add(new IndexChunk(chunkList.get(i)));
            readNum += IndexChunk.GetMessageNumber(chunkList.get(i));
        }

        Object[] aus = new Object[2];
        aus[0] = messageSkippedInFisrtChunk;
        aus[1] = result;
        return aus;
    }
}

class IndexChunk {
    public int MessageNumber;
    public int Offset;

    public IndexChunk(long chunk) {
        this.MessageNumber = GetMessageNumber(chunk);
        this.Offset = GetOffset(chunk);
    }

    public static int GetOffset(long chunk) {
        return (int) (chunk >> 32);
    }

    public static int GetMessageNumber(long chunk) {
        return (int) (chunk & 0x00000000ffffffff);
    }

    public static long GetChunk(int offset, int messageNumber) {
        long off = offset;
        off = off << 32;
        off = off | messageNumber;
        return off;
    }

    public static long AddMessage(long chunk, int num) {
        return GetChunk(GetOffset(chunk), GetMessageNumber(chunk) + num);
    }

    public static long AddOffset(long chunk, int offsetDelta) {
        return GetChunk(GetOffset(chunk) + offsetDelta, GetMessageNumber(chunk));
    }
}
