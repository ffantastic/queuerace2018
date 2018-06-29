package io.openmessaging;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Index {

    private final ConcurrentHashMap<String, List<IndexChunk>> table = new ConcurrentHashMap<>();

    public void Update(String queueName, int offset, int length, int messageNumber) {
        List<IndexChunk> chunkList = table.get(queueName);
        if(chunkList == null){
            table.put(queueName,chunkList);
        }

        chunkList.add(new IndexChunk(offset,length,messageNumber));
    }

}

class IndexChunk {
    public int Offset;
    public int Length;
    public int MessageNumber;

    public IndexChunk(int offset, int length, int messageNumber) {
        this.Offset = offset;
        this.Length = length;
        this.MessageNumber = messageNumber;
    }
}
