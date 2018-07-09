package io.openmessaging;

import java.nio.ByteBuffer;

class IndexChunk {
    // offset(int) + length(short)
    public String Filename;
    public int MessageStartIndex;
    public int MessageNum = 0;
    public String IndexFilename;
    private int[] offsets;
    private int indexOffset;
    private static final int CHUNK_SIZE = 400;

    public IndexChunk(int msgStartPos, String filename) {
        this.Filename = filename;
        this.MessageStartIndex = msgStartPos;
        this.offsets = new int[CHUNK_SIZE];
    }

    public boolean IndexMessage(int msgOffset) {
        offsets[MessageNum++] = msgOffset;
        if (MessageNum == CHUNK_SIZE) {
            return true;
        }

        return false;
    }

    public IndexResult Lookup(int offsetStart, int offsetEnd) {
        IndexResult result = new IndexResult();
        result.fileName = this.Filename;
        result.offsets = new int[offsetEnd - offsetStart + 1];
        for (int i = offsetStart; i <= offsetEnd; i++) {
            result.offsets[i - offsetStart] = offsets[i - MessageStartIndex];
        }

        return result;
    }

    public int GetChunkSize() {
        return 2 + MessageNum * 4;
    }

    public void DumpIndexAndReleaseResource(ByteBuffer buffer, String indexFilename, int offset) {
        this.indexOffset = offset;
        this.IndexFilename = indexFilename;
        int len = MessageNum;
        if (len > 0) {
            buffer.putShort((short) len);
            for (int i = 0; i < len; i++) {
                buffer.putInt(offsets[i]);
            }
        }

        offsets = null;
    }

    public synchronized void LoadIndex(MessageReader indexReader) {
        if (!IsLoaded()) {
            offsets = indexReader.ReadIndex(indexOffset);
        }
    }


    public boolean IsLoaded() {
        return offsets != null;
    }
}
