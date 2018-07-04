package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Index {
    private static final int CHAIN_LENGTH = 200;
    private static final int CHAIN_SIZE = CHAIN_LENGTH * IndexChunk.CHUNK_SIZE;
    private final List<byte[]> chunkList = new ArrayList<>();
    private int currentWritePos = 0;
    private int mergedPos = -1;

    public Index() {
        this.AddNewChain();
    }

    public void AddNewChunk(int currentLocalOffset) {
        if (currentWritePos >= CHAIN_SIZE) {
            this.AddNewChain();
        }

        byte[] chain = chunkList.get(chunkList.size() - 1);
        Bytes.int2bytes(currentLocalOffset, chain, currentWritePos);
        Bytes.short2bytes((short) 1, chain, currentWritePos + 4);
        currentWritePos += IndexChunk.CHUNK_SIZE;
    }

    public void UpdateChunk() {
        int currentIndex = (chunkList.size() - 1) * CHAIN_LENGTH + currentWritePos / IndexChunk.CHUNK_SIZE - 1;
        this.GetAndUpdateChunkNumber(currentIndex, 1, true);
    }

    public void Merge(int globalStartOffset) {
        int totalChunkNumber = (chunkList.size() - 1) * CHAIN_LENGTH + currentWritePos / IndexChunk.CHUNK_SIZE;
        while (mergedPos + 1 < totalChunkNumber) {
            mergedPos++;
            this.GetAndUpdateChunkOffset(mergedPos, globalStartOffset, true);
        }
    }

    public Object[] Lookup(int start, int num) {
        List<IndexChunk> result = new ArrayList<>();
        int curMessageIndex = -1;
        int messageSkippedInFisrtChunk = 0;

        int i = 0;
        int totalChunkNumber = (chunkList.size() - 1) * CHAIN_LENGTH + currentWritePos / IndexChunk.CHUNK_SIZE;
        for (; i < totalChunkNumber; i++) {
            int messageNumber = this.GetAndUpdateChunkNumber(i, 0, false);
            curMessageIndex += messageNumber;
            if (curMessageIndex >= start) {
                messageSkippedInFisrtChunk = messageNumber - curMessageIndex + start - 1;
                break;
            }
        }

        if (i == totalChunkNumber) {
            // not exist
            return null;
        }

        int currentChunkOffset = this.GetAndUpdateChunkOffset(i, 0, false);
        int currentChunkNumber = this.GetAndUpdateChunkNumber(i, 0, false);
        result.add(new IndexChunk(currentChunkNumber, currentChunkOffset));
        int readNum = currentChunkNumber - messageSkippedInFisrtChunk;
        i++;
        for (; i < totalChunkNumber; i++) {
            if (readNum >= num) {
                break;
            }

            currentChunkOffset = this.GetAndUpdateChunkOffset(i, 0, false);
            currentChunkNumber = this.GetAndUpdateChunkNumber(i, 0, false);
            result.add(new IndexChunk(currentChunkNumber, currentChunkOffset));
            readNum += currentChunkNumber;
        }

        Object[] aus = new Object[2];
        aus[0] = messageSkippedInFisrtChunk;
        aus[1] = result;
        return aus;
    }

    private void AddNewChain() {
        byte[] chain = new byte[CHAIN_SIZE];
        this.chunkList.add(chain);
        currentWritePos = 0;
    }

    private int GetAndUpdateChunkOffset(int index, int increment, boolean isUpdate) {
        int chain = index / CHAIN_LENGTH;
        int pos = (index % CHAIN_LENGTH) * IndexChunk.CHUNK_SIZE;
        int curValue = Bytes.bytes2int(chunkList.get(chain), pos);
        if (isUpdate) {
            Bytes.int2bytes(curValue + increment, chunkList.get(chain), pos);
        }
        return curValue;
    }

    private short GetAndUpdateChunkNumber(int index, int increment, boolean isUpdate) {
        int chain = index / CHAIN_LENGTH;
        int pos = (index % CHAIN_LENGTH) * IndexChunk.CHUNK_SIZE + 4;
        short curValue = Bytes.bytes2Short(chunkList.get(chain), pos);
        if (isUpdate) {
            Bytes.short2bytes((short) (curValue + increment), chunkList.get(chain), pos);
        }
        return curValue;
    }
}

class IndexChunk {
    // offset(int) + length(short)
    public static final int CHUNK_SIZE = 6;
    public int MessageNumber;
    public int Offset;

    public IndexChunk(int msgNumber, int offset) {
        this.MessageNumber = msgNumber;
        this.Offset = offset;
    }
}
