package os.gfs;

import java.util.Hashtable;
import java.util.LinkedList;

class ChunkDetails{
    int addresses[];
    String chunkName;
    public ChunkDetails() {
        // Need to change for configurable replication factor, use var length arguments or string as argument
        addresses = new int[GFS.replicationFact];
    }
}

class FileDetails{
    int chunkNum;
    int synCnt; // Count being maintained while syncing from chunksever.
    int status; // file status
    int filesize;
    LinkedList<ChunkDetails> chunkData;
    FileDetails(int num, int istatus, int size){
        chunkNum = num;
        chunkData = new LinkedList<ChunkDetails>();
        synCnt = 0;
        status = istatus;
        filesize = size;
    }
}

public abstract class MasterNamespace {
    public static Hashtable<String, FileDetails> namespace = new Hashtable<String, FileDetails>();

}
