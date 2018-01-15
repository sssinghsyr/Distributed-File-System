package os.gfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;

class SlaveAddress{
    String ipAddress;
    int port;
    SlaveAddress(String addr, int p){
        ipAddress = addr;
        port = p;
    }
}

class FileData{
    String name;
    int filesize;
    int chunkCnt;
    int clusterId[][];
    FileData(int size){
        filesize = size;
        chunkCnt = (size/GFS.chunkSize) + 1;
        clusterId = new int[chunkCnt][GFS.replicationFact];
    }
}

@SuppressWarnings("serial")
class PutCommandBuffer implements Serializable{
    String command;
    String filename;
    int chunkCnt;
    String[] chunkName;
    int clusterId[][];
    int filesize;
    int errorCode;
    PutCommandBuffer(String comm, String name, int cnt, int size) {
        clusterId = new int[cnt][GFS.replicationFact];
        chunkName = new String[cnt];
        command = comm;
        filename = name;
        chunkCnt = cnt;
        filesize = size;
        errorCode = GFS.SUCCESS;
    }
    public PutCommandBuffer(PutCommandBuffer command2) {
        // TODO Auto-generated constructor stub
        command = command2.command;
        filename = command2.filename;
        chunkCnt = command2.chunkCnt;
        clusterId = new int[chunkCnt][GFS.replicationFact];
        chunkName = new String[chunkCnt];
        filesize = command2.filesize;
    }
}

@SuppressWarnings("serial")
class ReadCommandBuffer implements Serializable{
    String filename;
    int startChunkIdx;
    int chunksCnt;
    int[][] chunksList;
    int filesize;
    int errorCode;
    int seek;
    int byteToRead;
    ReadCommandBuffer(String name, int start, int cnt, int iseek, int ibyteToRead) {
        filename = name;
        startChunkIdx = start;
        chunksCnt = cnt;
        errorCode = GFS.SUCCESS;
        chunksList = new int[cnt][GFS.replicationFact];
        filesize = 0;
        seek = iseek;
        byteToRead = ibyteToRead;
    }
    ReadCommandBuffer(ReadCommandBuffer buffer) {
        filename = buffer.filename;
        startChunkIdx = buffer.startChunkIdx;
        chunksCnt = buffer.chunksCnt;
        errorCode = GFS.SUCCESS;
        chunksList = new int[buffer.chunksCnt][GFS.replicationFact];
        filesize = 0;
    }   
}

@SuppressWarnings("serial")
class FetchCommandBuffer implements Serializable{
    String filename;
    int chunksCnt;
    int[][] chunksList;
    int errorCode;
    int filesize;
    FetchCommandBuffer(String name) {
        filename = name;
        chunksCnt = 0;
        errorCode = GFS.SUCCESS;
        chunksList = null;
        filesize = 0;
    }
}

@SuppressWarnings("serial")
class SlavePutInstructBuffer implements Serializable{
    String filename;
    int chunksize;
    String chunkName;
    byte[] chunkData;
    int totalChunks;
    int totalFileSize;
    int errorCode;
    SlavePutInstructBuffer(String ifilename, String name, int totChunks, byte[] chunkBuffer, int size) {
        filename = ifilename;
        chunkName = name;
        chunksize = chunkBuffer.length;
        chunkData = new byte[chunksize];
        chunkData = chunkBuffer;
        totalChunks = totChunks;
        totalFileSize = size;
        errorCode = GFS.SUCCESS;
    }
}

@SuppressWarnings("serial")
class SlaveReadInstructBuffer implements Serializable{
    String chunkName;
    int seekVal;
    int bytestoRead;
    int errorCode;
    byte[] chunkData;
    SlaveReadInstructBuffer(String name, int seek, int size) {
        chunkName = name;
        seekVal = seek;
        bytestoRead = size;
        chunkData = null;
        errorCode = GFS.SUCCESS;
    }
}

public class ClientMachine {
    
    public ClientMachine() {
        GFS.htSlaveTable = new Hashtable<Integer, SlaveAddress>();
    }

    public void put() throws IOException{
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        SlavePutInstructBuffer spibObj = null;
        String localpath = GFS.LocalFilePath;
        File myFile = new File (localpath);
        byte [] bytearray = new byte [GFS.chunkSize];

        int filesize = (int)myFile.length();
        int residueFileSize = filesize;
        FileData fdObj = new FileData(filesize);
        fdObj.name = myFile.getName();

        //Fetch meta data from Master machine
        PutCommandBuffer result = FetchMetaData(fdObj);
        if(result == null) {
            System.out.println("Error! Wrong transcode received!");
            return;
        }
        if(HandleMasterResult(result.errorCode) != GFS.SUCCESS)
            return;
        
        int iSlaveMachineId = 0;
        try {
            fis = new FileInputStream(myFile);
            bis = new BufferedInputStream(fis);
            for(int chunkIdx=0; chunkIdx < result.chunkCnt; chunkIdx++) {
                
                if(residueFileSize > GFS.chunkSize) {
                    bytearray = new byte [GFS.chunkSize];
                    bis.read(bytearray,0,bytearray.length);
                }
                else {
                    bytearray = new byte [residueFileSize];
                    bis.read(bytearray,0,bytearray.length);
                }
                residueFileSize -= GFS.chunkSize;
                for(int rfIdx=0; rfIdx < GFS.replicationFact; rfIdx++) {
                    System.out.println("ClusterId ="+result.clusterId[chunkIdx][rfIdx]);
                    iSlaveMachineId = result.clusterId[chunkIdx][rfIdx];
                    spibObj = new SlavePutInstructBuffer(
                            result.filename, result.chunkName[chunkIdx]+"_"+rfIdx, result.chunkCnt, bytearray, result.filesize);
                    SendDataToSlave(iSlaveMachineId, spibObj);
                }
            }           
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if (bis != null) bis.close();
        }
    }

    private void SendDataToSlave(int iSlaveMachineId, SlavePutInstructBuffer spibObj) {
        
        Socket sock = null;
        SlaveAddress saObj = GFS.htSlaveTable.get(iSlaveMachineId);
        if(saObj == null) {
            System.out.println("Error! : No data found for machine_id = "+iSlaveMachineId);
            return;
        }
        try {
            sock = new Socket(saObj.ipAddress, saObj.port);
            OutputStream os = sock.getOutputStream();
            
            byte[] obuffer = BytesUtil.toByteArray(spibObj);
            
            //Sending transcode first
            TranscodeHdr thObj = new TranscodeHdr(GFS.TRANS_PUT_CHUNCK_REQ, obuffer.length);
            byte[] transBuff = BytesUtil.toByteArray(thObj); 
            os.write(transBuff, 0, transBuff.length);
            os.write(obuffer,0,obuffer.length);
            os.flush();
            System.out.println("File sent to machine: " + iSlaveMachineId+ ", sock :"+sock);
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if (sock != null)
                try {
                    sock.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
        
    }

    private PutCommandBuffer FetchMetaData(FileData file) throws IOException{
        Socket sock = null;
        PutCommandBuffer result = null;
        int bytesRead;
        try {
            sock = new Socket(GFS.MasterIP, GFS.MasterPort);
            OutputStream os = sock.getOutputStream();
            InputStream is = sock.getInputStream();
            
            PutCommandBuffer command = new PutCommandBuffer("put", GFS.DestFilePath+"\\"+file.name, file.chunkCnt, file.filesize);
            byte[] obuffer = BytesUtil.toByteArray(command);
            
            //Sending transcode first
            TranscodeHdr thObj = new TranscodeHdr(GFS.TRANS_PUT_COMMAND, obuffer.length);
            byte[] transBuff = BytesUtil.toByteArray(thObj); 
            os.write(transBuff, 0, transBuff.length);
            os.write(obuffer,0,obuffer.length);
            os.flush();
            
            byte [] transcodeBuff = new byte[79];
            bytesRead = is.read(transcodeBuff, 0, transcodeBuff.length);
            if(bytesRead < 0)
                return null;
            try {
                thObj = (TranscodeHdr) BytesUtil.toObject(transcodeBuff);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if(thObj.transcode != GFS.TRANS_PUT_COMMAND_REPLY)
                return null;
            int messageSize = thObj.nextMsgSize;
            
            byte[] ibuffer = GFS.readAllData(is, messageSize);
/*          byte[] ibuffer = new byte [messageSize];
            bytesRead = is.read(ibuffer,0,ibuffer.length);*/
            result = (PutCommandBuffer) BytesUtil.toObject(ibuffer);
            
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (ConnectException e1) {
            System.out.println("Error! Connection refused by Master");
        }
        finally {
            if (sock != null) sock.close();
        }
        return result;
    }

    private int HandleMasterResult(int retValue) {
        // TODO Auto-generated method stub
        switch(retValue) {
        case GFS.ERR_NO_SLAVE_CONN:
            System.out.println("Error! GFS infrastructure not running!");
            return 1;
        case GFS.ERR_WRONG_FILE_NAME:
            System.out.println("Error! Destination file is incorrect!");
            return 1;
        case GFS.ERR_FILE_NT_SYNCED:
            System.out.println("Error! Destination file is incomplete, chunk address not present.. waiting for chunkservers to connect!");
            return 1;
        case GFS.ERR_FILE_SIZE_EX:
            System.out.println("Read Error! seek + byteRead is greater than the file size");
            return retValue;
        case GFS.SUCCESS:
            break;
        }
        return GFS.SUCCESS;
        
    }

    public void read(int seekValue, int byteRead) {
        FileOutputStream recvfos = null;
        BufferedOutputStream recvbos = null;
        String fileName = null;
        String tmpfileName = null;
        
        //Calculate start chunk index based upon given seek value.
        int start = seekValue / GFS.chunkSize;
        int count = ((byteRead + seekValue) /  GFS.chunkSize ) - start + 1;
        //Fetch meta data from Master machine
        ReadCommandBuffer input = new ReadCommandBuffer(GFS.DestFilePath, start, count, seekValue, byteRead);
        ReadCommandBuffer result = null;
        try {
            result = (ReadCommandBuffer)FetchMetaData(input, GFS.TRANS_MASTER_READ_DATA_REQ);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        int retVal = HandleMasterResult(result.errorCode);
        if(retVal != GFS.SUCCESS) {
            if(retVal == GFS.ERR_FILE_SIZE_EX)
                System.out.println("File size = "+result.filesize+ " bytes.");
            return;
        }
        
        int seek = 0;
        int size = 0;
        try {   
            String fullPath = result.filename;
            int index = fullPath.lastIndexOf("\\");
            fileName = fullPath.substring(index + 1);
            tmpfileName = GFS.localPath+"\\"+fileName+".tmp."+System.nanoTime();
            //Creating temp file to write received data buffer from multiple chunkservers.
            recvfos = new FileOutputStream(tmpfileName);
            recvbos = new BufferedOutputStream(recvfos);
            
            for(int cidx=0; cidx < count; cidx++) {
                if(cidx == 0) {
                    //start chunk
                    seek = seekValue - (start * GFS.chunkSize);
                    if((seek + byteRead) > GFS.chunkSize)
                        size = GFS.chunkSize - seek;
                    else
                        size = byteRead;
                }
                else if(cidx == (count -1)) {
                    //last chunk seek
                    seek = 0;
                    size = (seek + byteRead )% GFS.chunkSize;
                }else {
                    seek = 0;
                    size = GFS.chunkSize;
                }           
                ReadFromChunkServ(result.chunksList[cidx], fileName+"_"+cidx, seek, size, recvbos);
            }
            recvbos.flush();
            System.out.println("Read complete! Temporary file present at -["+tmpfileName+"]");
        }catch(IOException e) {
            e.printStackTrace();
        }
    }

    private void ReadFromChunkServ(int[] chunksList, String chunkName, int seekValue, int byteRead, BufferedOutputStream recvbos) {
        Socket sock = null;
        int bytesRead;
        SlaveAddress saObj = null;
        
        int machine_id = 0;
        int rfIdx = 0;
        boolean connected = false;
        for(rfIdx=0; rfIdx<GFS.replicationFact; rfIdx++) {
            System.out.println("SSS rfIdx:"+rfIdx);
            connected = true;
            if(chunksList[rfIdx] != 0) {
                machine_id = chunksList[rfIdx];
                System.out.println("machine id="+machine_id);
                saObj = GFS.htSlaveTable.get(machine_id);
                if(saObj == null) {
                    System.out.println("Error not configured! : No data found for machine_id = "+machine_id);
                    continue;
                }
                try {
                    sock = new Socket(saObj.ipAddress, saObj.port);
                }catch(ConnectException | UnknownHostException e){
                    //Try another clusterserver if connection is refused.
                    connected = false;
                    continue;
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                break;
            }   
        }
        if(machine_id == 0) {
            //Cannot hit this case as server will return error in this case.
            System.out.println("Error! Bad address received from master machine");
            System.exit(0);
        }
        if(connected == false) {
            System.out.println("Error in retrieving ["+chunkName+"] chunk.");
            System.out.print("Cannot connect to machine_id = ");
            for(int cnt=0; cnt<GFS.replicationFact; cnt++) {
                System.out.print(chunksList[cnt]+";");
            }
            System.exit(0);
        }
        try {
            OutputStream os = sock.getOutputStream();
            InputStream is = sock.getInputStream();
            SlaveReadInstructBuffer sriBuff = new SlaveReadInstructBuffer(chunkName+"_"+rfIdx, seekValue, byteRead);
            byte[] obuffer = BytesUtil.toByteArray(sriBuff);
            
            //Sending transcode first
            TranscodeHdr thObj = new TranscodeHdr(GFS.TRANS_SLAVE_READ_DATA_REQ, obuffer.length);
            byte[] transBuff = BytesUtil.toByteArray(thObj); 
            os.write(transBuff, 0, transBuff.length);
            os.write(obuffer,0,obuffer.length);
            os.flush();
            
            //Receive response from the chunkservers.
            byte [] recvtransBuff = new byte[79];
            bytesRead = is.read(recvtransBuff, 0, recvtransBuff.length);
            if(bytesRead < 0)
                return;
            try {
                thObj = (TranscodeHdr) BytesUtil.toObject(recvtransBuff);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if(thObj.transcode != GFS.TRANS_SLAVE_READ_DATA_REP)
                return;
            int messageSize = thObj.nextMsgSize;
            byte[] ibuffer = GFS.readAllData(is, messageSize);
/*          byte[] ibuffer = new byte [messageSize];
            bytesRead = is.read(ibuffer,0,ibuffer.length);*/
            SlaveReadInstructBuffer result = (SlaveReadInstructBuffer) BytesUtil.toObject(ibuffer);
            if(HandleMasterResult(result.errorCode) != GFS.SUCCESS)
                return;
            recvbos.write(result.chunkData);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if (sock != null)
                try {
                    sock.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
        
    }

    private Object FetchMetaData(Object input, int transcode) throws IOException {
        Socket sock = null;
        ReadCommandBuffer rcbObj = null;
        FetchCommandBuffer fcbObj = null;
        byte[] obuffer = null;
        Object result = null;
        try {
            sock = new Socket(GFS.MasterIP, GFS.MasterPort);
            OutputStream os = sock.getOutputStream();
            InputStream is = sock.getInputStream();
            int bytesRead = 0;
            
            if(transcode == GFS.TRANS_MASTER_READ_DATA_REQ) {
                rcbObj = (ReadCommandBuffer)input;
                obuffer = BytesUtil.toByteArray(rcbObj);
            }
            else if(transcode == GFS.TRANS_FULL_FILE_MET_REQ) {
                fcbObj = (FetchCommandBuffer)input;
                obuffer = BytesUtil.toByteArray(fcbObj);
            }
            else {
                System.out.println("Error! FetchMetaData, unimplemented transcode");
                return null;
            }
            //Sending transcode first
            TranscodeHdr thObj = new TranscodeHdr(transcode, obuffer.length);
            byte[] transBuff = BytesUtil.toByteArray(thObj); 
            os.write(transBuff, 0, transBuff.length);
            os.write(obuffer,0,obuffer.length);
            os.flush();
            
            // Read response transcode
            
            byte [] transcodeBuff = new byte[79];
            bytesRead = is.read(transcodeBuff, 0, transcodeBuff.length);
            if(bytesRead < 0)
                return null;
            try {
                thObj = (TranscodeHdr) BytesUtil.toObject(transcodeBuff);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if(thObj.transcode != GFS.TRANS_MASTER_READ_DATA_REP && thObj.transcode != GFS.TRANS_FULL_FILE_MET_REP)
                return null;
            int messageSize = thObj.nextMsgSize;
            
            byte[] ibuffer = GFS.readAllData(is, messageSize);
/*          byte[] ibuffer = new byte [obuffer.length];
            bytesRead = is.read(ibuffer,0,ibuffer.length);*/
            if(transcode == GFS.TRANS_MASTER_READ_DATA_REQ) {
                result = (ReadCommandBuffer) BytesUtil.toObject(ibuffer);
            }
            else if(transcode == GFS.TRANS_FULL_FILE_MET_REQ) {
                result = (FetchCommandBuffer) BytesUtil.toObject(ibuffer);
            }
            
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if (sock != null) sock.close();
        }
        return result;
    }

    public void fetch() {
        /* Read full file from GFS
         */
        FileOutputStream recvfos = null;
        BufferedOutputStream recvbos = null;
        
        FetchCommandBuffer result = null;
        FetchCommandBuffer input = new FetchCommandBuffer(GFS.DestFilePath);
        
        try {
            result = (FetchCommandBuffer) FetchMetaData(input, GFS.TRANS_FULL_FILE_MET_REQ);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        int retVal = HandleMasterResult(result.errorCode);
        if(retVal != GFS.SUCCESS)
            return;
        
        try {   
            String fullPath = result.filename;
            int index = fullPath.lastIndexOf("\\");
            String fileName = fullPath.substring(index + 1);
            String tmpfileName = GFS.localPath+"\\"+fileName;
            //Creating temp file to write received data buffer from multiple chunkservers.
            recvfos = new FileOutputStream(tmpfileName);
            recvbos = new BufferedOutputStream(recvfos);
            
            for(int cidx=0; cidx < result.chunksCnt; cidx++) {
                int size = 0;
                if(cidx == (result.chunksCnt -1)) {
                    //last chunk seek
                    size = result.filesize % GFS.chunkSize;
                }else {
                    size = GFS.chunkSize;
                }           
                ReadFromChunkServ(result.chunksList[cidx], fileName+"_"+cidx, 0, size, recvbos);
            }
            recvbos.flush();
            System.out.println("Read complete! fetched file present at -["+tmpfileName+"]");
        }catch(IOException e) {
            e.printStackTrace();
        }

    }
}










