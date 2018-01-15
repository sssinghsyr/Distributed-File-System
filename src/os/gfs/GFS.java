package os.gfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;

public abstract class GFS {
    
    public static final int CLIENT = 3000;
    public static final int MASTER = 1000;
    public static final int SLAVE = 2000;
    
    public static int FILE_ID_COUNTER = 1234;

    public static final int STATUS_RUNNING = 0;
    public static final int STATUS_CONNEDTED = 1;
    public static final int STATUS_NO_HB = 2;
    public static final int STATUS_DEAD = 3;
    public static final int STATUS_DEAD_AND_REPLICATED = 4;
    
    //Definition of trancode
    public static final int TRANS_SLAVE_CONNECTION = 1001;
    public static final int TRANS_PUT_COMMAND = 1011;
    public static final int TRANS_PUT_COMMAND_REPLY = 1012;
    public static final int TRANS_PUT_CHUNCK_REQ = 1013;
    public static final int TRANS_META_DATA_IN = 1014;
    public static final int TRANS_MASTER_READ_DATA_REQ = 1015;
    public static final int TRANS_MASTER_READ_DATA_REP = 1016;
    public static final int TRANS_SLAVE_READ_DATA_REQ = 1017;
    public static final int TRANS_SLAVE_READ_DATA_REP = 1018;
    public static final int TRANS_FULL_FILE_MET_REQ = 1019;
    public static final int TRANS_FULL_FILE_MET_REP = 1020;
    
    //File status
    public static final int FILE_STATUS_NEW = 5001;
    public static final int FILE_STATUS_INCOMPLETE = 5002;
    public static final int FILE_STATUS_SYNCED = 5003;
    
    //Error codes
    public static final int SUCCESS = 0;
    public static final int ERR_WRONG_FILE_NAME = 2001;
    public static final int ERR_NO_SLAVE_CONN = 2002;
    public static final int ERR_FILE_NT_SYNCED = 2003;
    public static final int ERR_FILE_SIZE_EX = 2004;
    
    public static final int HB_MESS_SIZE = 66;
    
    public static String MasterIP;
    public static int MasterPort;
    
    public static String SlaveIP;
    public static int SlavePort;
    
    public static int HeartBeatIntrv;
    
    public static int id;
    public static String localPath;
    
    public static Hashtable<Integer, SlaveNode> hConnectedSlave = new Hashtable<Integer, SlaveNode>();
    public static ArrayList<Integer> alRunningSlave = new ArrayList<Integer>();
    public static LinkedList<Integer> configuredSlaveNodes = new LinkedList<Integer>();
    
    public static Hashtable<Integer, SlaveAddress> htSlaveTable= null;
    
    public static String LocalFilePath;
    public static String DestFilePath;
    
    public static int chunkSize;
    public static int replicationFact;
    
    
    /* Slave part ---------------- */
    public static Semaphore cliSemaphore = new Semaphore(0);
    
    public static byte[] readAllData(InputStream is, int messageSize) {
        int toRead = 0;
        int bytesRead = 0;
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );

        toRead = messageSize;
        try {
            while(toRead > 0) {
                int eachRead = toRead > 1024 ? 1024 : toRead;
                byte [] readMessBuff = new byte[eachRead];
                bytesRead = is.read(readMessBuff, 0, eachRead);
                if(bytesRead <= 0)
                    break;
                outputStream.write(readMessBuff, 0, bytesRead);
                toRead -= bytesRead;
            }
        }catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Inside readAll: messagesize="+messageSize+" outputsize="+outputStream.toByteArray().length);
        return outputStream.toByteArray();
    }
}





