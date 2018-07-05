package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class Index {
    private List<IndexChunk> flushedChunks = new ArrayList<>();
    private IndexChunk currentChunk;
    private volatile int messageNum = 0;
    private Bucket bucket;

    public Index(Bucket bucket) {
        this.bucket = bucket;
    }

    public boolean IndexMessage(int offset) {
        if (currentChunk == null) {
            currentChunk = new IndexChunk(messageNum, bucket.CurrentFileName);
        }

        messageNum++;
        return currentChunk.IndexMessage(offset);
    }

    public int GetCurChunkSize() {
        return currentChunk == null ? 0 : currentChunk.GetChunkSize();
    }

    public void FlushCurrentChunk(ByteBuffer buffer, int offset) {
        if (currentChunk != null) {
            currentChunk.DumpIndexAndReleaseResource(buffer, bucket.CurrentFileName, offset);
            if (currentChunk.MessageNum != 0) {
                flushedChunks.add(currentChunk);
            }

            currentChunk = null;
        }
    }

    public List<IndexResult> Lookup(int startMsg, int len) {
        if (flushedChunks.size() == 0) {
            return null;
        }

        List<IndexResult> result = new ArrayList<>();
        int chunkIdx = 0, readedMsg = 0, tail = startMsg + len - 1;

        for (; chunkIdx < flushedChunks.size(); chunkIdx++) {
            IndexChunk chunk = flushedChunks.get(chunkIdx);
            int chunkTail = chunk.MessageStartIndex + chunk.MessageNum - 1;
            if (chunk.MessageStartIndex > tail || chunkTail < startMsg) {
                continue;
            }

            if (!chunk.IsLoaded()) {
                this.LoadChunk(chunk);
            }
            int offStart = Math.max(chunk.MessageStartIndex, startMsg);
            int offEnd = Math.min(chunkTail, tail);
            result.add(chunk.Lookup(offStart, offEnd));
            readedMsg += (offEnd - offStart + 1);
            if (readedMsg == len) {
                break;
            }
        }

        if (readedMsg == 0) {
            result = null;
        }

        return result;
    }

    private void LoadChunk(IndexChunk chunk) {
        chunk.LoadIndex(bucket.ReaderMap.get(chunk.IndexFilename));
    }

}

class IndexResult {
    public String fileName;
    public int[] offsets;
}

