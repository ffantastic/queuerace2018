package io.openmessaging;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Index {

    private final List<IndexChunk> chunkList = new ArrayList<>();
    private int mergedPos = -1;


    public void AddNewChunk(int currentLocalOffset) {
        chunkList.add(new IndexChunk(currentLocalOffset, 1));
    }

    public void UpdateChunk() {
        IndexChunk lastChunk = chunkList.get(chunkList.size() - 1);
        lastChunk.MessageNumber++;
    }

    public void Merge(int globalStartOffset) {
        while (mergedPos + 1 < chunkList.size()) {
            mergedPos++;
            chunkList.get(mergedPos).Offset += globalStartOffset;
        }
    }

    public Object[] Lookup(int start, int num) {
        List<IndexChunk> result = new ArrayList<>();
        int curMessageIndex = -1;
        int messageSkippedInFisrtChunk = 0;

        int i = 0;
        for (; i < chunkList.size(); i++) {
            curMessageIndex += chunkList.get(i).MessageNumber;
            if (curMessageIndex >= start) {
                messageSkippedInFisrtChunk = curMessageIndex - start;
                break;
            }
        }

        if (i == chunkList.size()) {
            // not exist
            return null;
        }

        result.add(chunkList.get(i));
        int readNum = chunkList.get(i).MessageNumber - messageSkippedInFisrtChunk;
        i++;
        for (; i < chunkList.size(); i++) {
            result.add(chunkList.get(i));
            readNum += chunkList.get(i).MessageNumber;
            if (readNum >= num) {
                break;
            }
        }

        Object[] aus = new Object[2];
        aus[0] = messageSkippedInFisrtChunk;
        aus[1] = result;
        return aus;
    }
}

class IndexChunk {
    public int Offset;
    public int MessageNumber;

    public IndexChunk(int offset, int messageNumber) {
        this.Offset = offset;
        this.MessageNumber = messageNumber;
    }
}
