package os.gfs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

@SuppressWarnings("serial")
class HeartBeatMessage implements Serializable{
    int machine_id;
    HeartBeatMessage(int id) {
        machine_id = id;
    }
}

@SuppressWarnings("serial")
class FileMetaData implements Serializable{
    String filePath;
    int chunkIdx;
    int repIdx;
    int totalChunks;
    int totalFileSize;
    FileMetaData(String path, int cIdx, int rIdx, int totChunks, int size) {
        filePath = path;
        chunkIdx = cIdx;
        repIdx = rIdx;
        totalChunks = totChunks;
        totalFileSize = size;
    }
}

public class SlaveMachine {
    @SuppressWarnings("deprecation")
    public SlaveMachine() {
        SlaveServer ssObj = null;
        //  Connect with Master machine.
        try {
            initialize();
            ssObj = new SlaveServer();
            ConnectMaster();
        } catch (IOException e) {
            System.out.println("Error! Cannot connect to Master");
            ssObj.mThread.stop();
        }
    }

    private void initialize() {
        // TODO Auto-generated method stub
        String dirpath = GFS.localPath+"//"+GFS.id;
        File theDir = new File(dirpath);

        // if the directory does not exist, create it
        if (!theDir.exists()) {
            System.out.println("creating directory: " + theDir.getName());
            boolean result = false;

            try{
                theDir.mkdir();
                result = true;
            } 
            catch(SecurityException se){
                //handle it
            }        
            if(result) {    
                System.out.println("DIR created");  
            }
        }
        GFS.localPath = dirpath;
    }

    public void ConnectMaster () throws IOException {

        Socket sock = null;
        try {
            while(true) {
                int tryCnt = 6;
                while(tryCnt > 0) {
                    try {
                        sock = new Socket(GFS.MasterIP, GFS.MasterPort);
                    }catch(ConnectException e) {
                        System.out.println("Connection refused, master not up");
                        System.out.println("Will try after 10 sec");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                        tryCnt--;
                        continue;
                    }
                    System.out.println("Connecting...");
                    break;
                }
                if(tryCnt == 0) {
                    throw new IOException();
                }
                OutputStream os = sock.getOutputStream();
                // connect with master machine
                CnnctAndSndMetaDataToMstr(os);
                GFS.cliSemaphore.release(); /* This will start SlaveServer thread to listen for new commands */ 
                SendHeartBeat(os);
            }
        }
        finally {
            if (sock != null) sock.close();
        }
    }

    private void CnnctAndSndMetaDataToMstr(OutputStream os) throws IOException {
        File[] files = new File(GFS.localPath).listFiles();
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        //If this pathname does not denote a directory, then listFiles() returns null. 

        int cnt = 0;
        for (File file : files)
            if (file.isFile() && file.getName().contains("meta"))
                cnt++;
        ConnectionMessage message = new ConnectionMessage(GFS.id, cnt);
        byte[] buffer = BytesUtil.toByteArray(message);
        //Sending transcode first
        TranscodeHdr thObj = new TranscodeHdr(GFS.TRANS_SLAVE_CONNECTION, buffer.length);
        byte[] transBuff = BytesUtil.toByteArray(thObj); 
        os.write(transBuff, 0, transBuff.length);
        //os.flush();
        os.write(buffer,0,buffer.length);
        os.flush();
        
        for (File file : files) {
            if (file.isFile() && file.getName().contains("meta")) {
                try {
                    fis = new FileInputStream(file);
                    bis = new BufferedInputStream(fis);
                    
                    byte[] metBuff = new byte [(int) file.length()];
                    bis.read(metBuff,0, metBuff.length);
                    //Sending transcode first
                    TranscodeHdr thObj1 = new TranscodeHdr(GFS.TRANS_META_DATA_IN, metBuff.length);
                    byte[] transBuff1 = BytesUtil.toByteArray(thObj1); 
                    os.write(transBuff1, 0, transBuff1.length);
                    os.write(metBuff, 0, metBuff.length);
                    os.flush();
                    
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }finally {
                    if (fis != null) fis.close();
                    if (bis != null) bis.close();
                }
            }
        }
        
    }

    private void SendHeartBeat(OutputStream os) {
        // TODO Auto-generated method stub
        
        HeartBeatMessage hbObj = new HeartBeatMessage(GFS.id);
        byte[] buffer;
        try {
            buffer = BytesUtil.toByteArray(hbObj);
            while(true) {
                Thread.sleep(1000 * GFS.HeartBeatIntrv);
                try {
                os.write(buffer,0,buffer.length);
                os.flush();
                }catch (SocketException e) {
                    System.out.println("Error! Connection closed by Master");
                    return;
                }
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

}
