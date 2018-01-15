package os.gfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;


@SuppressWarnings("serial")
class ConnectionMessage implements Serializable{
    public int id;
    public int metaFileCnt;

    public ConnectionMessage(int x, int cnt) {
        id = x;
        metaFileCnt = cnt;
    }
}

@SuppressWarnings("serial")
class TranscodeHdr implements Serializable{
    int transcode;
    int nextMsgSize;

    TranscodeHdr(int trans, int size) {
        transcode = trans;
        nextMsgSize = size;
    }
}

public class MasterServer implements Runnable {

    public Thread mThread;

    public MasterServer() {
        mThread = new Thread(this);
        mThread.start();
    }
    @Override
    public void run(){
        // TODO Auto-generated method stub
        InputStream is = null;
        OutputStream os = null;
        ServerSocket servsock = null;
        Socket sock = null;
        ConnectionMessage message = null;
        PutCommandBuffer command = null;
        TranscodeHdr thObj = null;
        FileMetaData fmdObj = null;
        int bytesRead = 0;
        int transcode = 0;
        boolean continue_flag = false;
        try {
            servsock = new ServerSocket(GFS.MasterPort, 20);
            while (true) {
                System.out.println("Waiting...");
                try {
                    sock = servsock.accept();
                    System.out.println("Accepted connection : " + sock);
                    is = sock.getInputStream();
                    byte [] transcodeBuff = new byte[79];
                    bytesRead = is.read(transcodeBuff, 0, transcodeBuff.length);
                    try {
                        thObj = (TranscodeHdr) BytesUtil.toObject(transcodeBuff);
                    } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    transcode = thObj.transcode;
                    int messageSize = thObj.nextMsgSize;
                    System.out.println("Transcode received = "+transcode);

                    if(transcode == GFS.TRANS_SLAVE_CONNECTION) {
                        byte [] connMesBuffer = GFS.readAllData(is, messageSize);
/*                      byte [] connMesBuffer = new byte [messageSize];
                        bytesRead = is.read(connMesBuffer,0,connMesBuffer.length);*/
                        try {
                            message = (ConnectionMessage) BytesUtil.toObject(connMesBuffer);
                        } catch (ClassNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        int machine_id = message.id;
                        SlaveNode node = new SlaveNode(machine_id, ReadConfigXml.getAddressForNode(machine_id), 
                                GFS.STATUS_CONNEDTED, System.currentTimeMillis());
                        System.out.println(machine_id + " connected.");
                        GFS.hConnectedSlave.put(machine_id, node);

                        //Fetch MetaData from slave machines
                        int metaDataCnt = message.metaFileCnt;
                        for(int idx=0; idx<metaDataCnt; idx++) {
                            byte [] tBuff = new byte[79];
                            bytesRead = is.read(tBuff, 0, tBuff.length);
                            try {
                                thObj = (TranscodeHdr) BytesUtil.toObject(tBuff);
                            } catch (ClassNotFoundException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            transcode = thObj.transcode;
                            messageSize = thObj.nextMsgSize;                
                            if(transcode == GFS.TRANS_META_DATA_IN) {
                                byte [] metaFileBuff = GFS.readAllData(is, messageSize);
//                              byte [] metaFileBuff = new byte[messageSize];
                                try {
//                                  bytesRead = is.read(metaFileBuff,0,metaFileBuff.length);
                                    fmdObj = (FileMetaData) BytesUtil.toObject(metaFileBuff);
                                } catch (ClassNotFoundException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                                FileDetails fdObj = MasterNamespace.namespace.get(fmdObj.filePath);
                                if(fdObj!= null && (fdObj.status == GFS.FILE_STATUS_SYNCED || fdObj.status == GFS.FILE_STATUS_NEW))
                                    continue;
                                
                                System.out.println("Name: "+fmdObj.filePath+ " chunkIdx="+fmdObj.chunkIdx+" repIdx="+fmdObj.repIdx);
                                UpdateNameSpace(fmdObj, machine_id);
                            }
                            else {
                                System.out.println("Error! Wrong transcode received");
                            }
                        }
                        new MasterHBReceiver(machine_id, sock);
                        continue_flag = true;
                        continue;
                    }
                    else if(transcode == GFS.TRANS_PUT_COMMAND) {

                        byte [] connMesBuffer = GFS.readAllData(is, messageSize);
//                      byte [] connMesBuffer = new byte [messageSize];
//                      bytesRead = is.read(connMesBuffer,0,connMesBuffer.length);
                        try {
                            command = (PutCommandBuffer) BytesUtil.toObject(connMesBuffer);
                        } catch (ClassNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        PutCommandBuffer retCommand = ExecPutCommd(command);
                        os = sock.getOutputStream();
                        byte[] retCommandBuff = BytesUtil.toByteArray(retCommand);
                        //Sending transcode first
                        TranscodeHdr repThObj = new TranscodeHdr(GFS.TRANS_PUT_COMMAND_REPLY, retCommandBuff.length);
                        byte[] transBuff = BytesUtil.toByteArray(repThObj); 
                        os.write(transBuff, 0, transBuff.length);

                        os.write(retCommandBuff, 0, retCommandBuff.length);
                        os.flush();
                    }
                    else if(transcode == GFS.TRANS_MASTER_READ_DATA_REQ) {

                        byte [] readCommBuffer = GFS.readAllData(is, messageSize);
//                      byte [] readCommBuffer = new byte [messageSize];
                        ReadCommandBuffer commBuff = null;
//                      bytesRead = is.read(readCommBuffer,0,readCommBuffer.length);
                        try {
                            commBuff = (ReadCommandBuffer) BytesUtil.toObject(readCommBuffer);
                        } catch (ClassNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        ReadCommandBuffer retCommand = ExecReadCommd(commBuff);
                        os = sock.getOutputStream();
                        byte[] retCommandBuff = BytesUtil.toByteArray(retCommand);
                        //Sending transcode first
                        TranscodeHdr repThObj = new TranscodeHdr(GFS.TRANS_MASTER_READ_DATA_REP, retCommandBuff.length);
                        byte[] transBuff = BytesUtil.toByteArray(repThObj); 
                        os.write(transBuff, 0, transBuff.length);
                        
                        os.write(retCommandBuff, 0, retCommandBuff.length);
                        os.flush();
                    }
                    else if(transcode == GFS.TRANS_FULL_FILE_MET_REQ) {
                        byte [] readCommBuffer = GFS.readAllData(is, messageSize);
                        FetchCommandBuffer commBuff = null;
                        try {
                            commBuff = (FetchCommandBuffer) BytesUtil.toObject(readCommBuffer);
                        } catch (ClassNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        FetchCommandBuffer retCommand = ExecFetchCommd(commBuff);
                        os = sock.getOutputStream();
                        byte[] retCommandBuff = BytesUtil.toByteArray(retCommand);
                        //Sending transcode first
                        TranscodeHdr repThObj = new TranscodeHdr(GFS.TRANS_FULL_FILE_MET_REP, retCommandBuff.length);
                        byte[] transBuff = BytesUtil.toByteArray(repThObj); 
                        os.write(transBuff, 0, transBuff.length);
                        
                        os.write(retCommandBuff, 0, retCommandBuff.length);
                        os.flush();
                    }
                }
                finally {
                    if(continue_flag == true)
                        continue_flag = false;
                    else {
                        if (is != null) is.close();
                        if (sock!=null) sock.close();
                    }
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if (servsock != null)
                try {
                    servsock.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            if(sock != null)
                try {
                    sock.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
    }
    private ReadCommandBuffer ExecReadCommd(ReadCommandBuffer commBuff) {

        FileDetails fdObj = MasterNamespace.namespace.get(commBuff.filename);
        if(fdObj == null) {
            System.out.println("Wrong command :[read] received for file :"+commBuff.filename);
            commBuff.errorCode = GFS.ERR_WRONG_FILE_NAME;
            return commBuff;
        }
        // Check for file size. If requested size is greater than filesize, just return an error to client.
        if((commBuff.seek + commBuff.byteToRead) > fdObj.filesize) {
            System.out.println("Error! Requested file read is greater than the file size");
            commBuff.filesize = fdObj.filesize;
            commBuff.errorCode = GFS.ERR_FILE_SIZE_EX;
            return commBuff;
        }
        
        for(int cnt=0; cnt<commBuff.chunksCnt; cnt++) {
            commBuff.chunksList[cnt] = fdObj.chunkData.get(commBuff.startChunkIdx+cnt).addresses;
            int [] addresses = commBuff.chunksList[cnt];
            int total = 0;
            for(int i=0; i<GFS.replicationFact; i++)
                total += addresses[i];
            if(total == 0) {
                // Returning error, if all of the addresses are empty
                System.out.println("Error in :[read] command, File ["+commBuff.filename+"] not synced, missing chunk id:"+commBuff.startChunkIdx+cnt);
                commBuff.errorCode = GFS.ERR_FILE_NT_SYNCED;
                return commBuff;
            }
        }
        return commBuff;
    }

    private FetchCommandBuffer ExecFetchCommd(FetchCommandBuffer commBuff) {

        FileDetails fdObj = MasterNamespace.namespace.get(commBuff.filename);
        if(fdObj == null) {
            System.out.println("Wrong command :[read] received for file :"+commBuff.filename);
            commBuff.errorCode = GFS.ERR_WRONG_FILE_NAME;
            return commBuff;
        }
        commBuff.chunksCnt = fdObj.chunkNum;
        commBuff.filesize = fdObj.filesize;
        commBuff.chunksList = new int[commBuff.chunksCnt][GFS.replicationFact];
        for(int cnt=0; cnt < commBuff.chunksCnt; cnt++) {
            commBuff.chunksList[cnt] = fdObj.chunkData.get(cnt).addresses;
            int [] addresses = commBuff.chunksList[cnt];
            int total = 0;
            for(int i=0; i<GFS.replicationFact; i++)
                total += addresses[i];
            if(total == 0) {
                // Returning error, if all of the addresses are empty
                System.out.println("Error in :[fetch] command, File ["+commBuff.filename+"] not synced, missing chunk id:"+cnt);
                commBuff.errorCode = GFS.ERR_FILE_NT_SYNCED;
                return commBuff;
            }
        }
        return commBuff;
    }
    
    private void UpdateNameSpace(FileMetaData fmdObj, int machine_id) {
        // TODO Auto-generated method stub
        FileDetails fdObj = MasterNamespace.namespace.get(fmdObj.filePath);
        if(fdObj == null) {
            //File not present, create an entry for this new file
            fdObj = AddChunkDetInNameSpace(fmdObj);
        }
        fdObj.chunkData.get(fmdObj.chunkIdx).addresses[fmdObj.repIdx] = machine_id;
        // Update cnt of received chunks to sync all copies.
        fdObj.synCnt++;
        if(fdObj.synCnt == (fmdObj.totalChunks * GFS.replicationFact))
            fdObj.status = GFS.FILE_STATUS_SYNCED;
    }
    private FileDetails AddChunkDetInNameSpace(FileMetaData fmdObj) {
        // TODO Auto-generated method stub
        String fullPath = fmdObj.filePath;
        int index = fullPath.lastIndexOf("\\");
        String chunkName = fullPath.substring(index + 1);

        FileDetails fdObj = new FileDetails(fmdObj.totalChunks, GFS.FILE_STATUS_INCOMPLETE, fmdObj.totalFileSize);
        ChunkDetails cdObj = null;
        for(int chunkIdx=0; chunkIdx < fmdObj.totalChunks; chunkIdx++) {
            cdObj = new ChunkDetails();
            cdObj.chunkName = chunkName+"_"+chunkIdx;
            fdObj.chunkData.add(cdObj);
        }
        MasterNamespace.namespace.put(fmdObj.filePath, fdObj);

        return fdObj;
    }
    private PutCommandBuffer ExecPutCommd(PutCommandBuffer command) {

        PutCommandBuffer retCBObj = new PutCommandBuffer(command);

        String fullPath = command.filename;
        int index = fullPath.lastIndexOf("\\");
        String chunkName = fullPath.substring(index + 1);

        FileDetails fdObj = MasterNamespace.namespace.get(command.filename);
        if(fdObj != null) {
            System.out.println("Wrong command :["+command.command+"] received for file :"+command.filename);
            retCBObj.errorCode = GFS.ERR_WRONG_FILE_NAME;
            return retCBObj;
        }
        // TODO: Find available chunk space from the connected chunkservers
        int noConnIds = GFS.alRunningSlave.size();
        if(noConnIds < 1) {
            System.out.println("Error! Command :["+command.command+"] received but no slaves connected!");
            retCBObj.errorCode = GFS.ERR_NO_SLAVE_CONN;
            return retCBObj;
        }

        fdObj = new FileDetails(command.chunkCnt, GFS.FILE_STATUS_NEW, command.filesize);

        ChunkDetails cdObj = null;

        // Fetch clusterIds using random number
        int []clusIdxArr = new int[command.chunkCnt * GFS.replicationFact];
        Random rand = new Random();
        int Idx=0;
        for(Idx=0; Idx < clusIdxArr.length; Idx++) {
            clusIdxArr[Idx] = rand.nextInt(noConnIds);
        }

        int[] connectedIds = new int[noConnIds];
        Idx=0;
        while(noConnIds > 0) {
            connectedIds[Idx] = GFS.alRunningSlave.get(clusIdxArr[Idx]);
            Idx++;
            noConnIds--;
        }
        Idx=0;
        for(int chunkIdx=0; chunkIdx < command.chunkCnt; chunkIdx++) {
            cdObj = new ChunkDetails();
            for(int rfIdx=0; rfIdx < GFS.replicationFact; rfIdx++) {
                if(Idx > clusIdxArr.length)
                    break;
                retCBObj.clusterId[chunkIdx][rfIdx] = connectedIds[clusIdxArr[Idx]];
                cdObj.addresses[rfIdx] = connectedIds[clusIdxArr[Idx]];
                Idx++;
            }
            cdObj.chunkName = chunkName+"_"+chunkIdx;
            retCBObj.chunkName[chunkIdx] = cdObj.chunkName;
            fdObj.chunkData.add(cdObj);
        }
        MasterNamespace.namespace.put(command.filename, fdObj);
        return retCBObj;
    }

}
