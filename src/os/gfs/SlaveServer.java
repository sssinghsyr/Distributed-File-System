package os.gfs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.*;

public class SlaveServer implements Runnable{
    public Thread mThread;

    public SlaveServer() {
        mThread = new Thread(this);
        mThread.start();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        ServerSocket servsock = null;
        Socket sock = null;
        InputStream is = null;
        OutputStream os = null;
        int bytesRead = 0;
        TranscodeHdr thObj = null;

        try {
            GFS.cliSemaphore.acquire();
            servsock = new ServerSocket(GFS.SlavePort, 10, InetAddress.getByName(GFS.SlaveIP));
            while (true) {
                sock = servsock.accept();
                System.out.println("Accepted connection : " + sock);
                is = sock.getInputStream();
                os = sock.getOutputStream();
                byte [] transcodeBuff = new byte[79];
                bytesRead = is.read(transcodeBuff, 0, transcodeBuff.length);
                try {
                    thObj = (TranscodeHdr) BytesUtil.toObject(transcodeBuff);
                } catch (ClassNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                int transcode = thObj.transcode;
                int messageSize = thObj.nextMsgSize;
                System.out.println("Transcode received = "+transcode);
                if(transcode == GFS.TRANS_PUT_CHUNCK_REQ) {
                    //Handle put request from client here
                    SaveChunk(sock, messageSize);
                }
                else if(transcode == GFS.TRANS_SLAVE_READ_DATA_REQ) {
                    //Handle read request from client machine
                    SlaveReadInstructBuffer readObj = ReadChunk(sock, messageSize);
                    byte[] retCommandBuff = BytesUtil.toByteArray(readObj);

                    //Sending transcode first
                    TranscodeHdr repThObj = new TranscodeHdr(GFS.TRANS_SLAVE_READ_DATA_REP, retCommandBuff.length);
                    byte[] transBuff = BytesUtil.toByteArray(repThObj); 
                    os.write(transBuff, 0, transBuff.length);

                    os.write(retCommandBuff, 0, retCommandBuff.length);
                    os.flush();                 
                }
            }
        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if(sock != null)
                try {
                    sock.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
    }

    private SlaveReadInstructBuffer ReadChunk(Socket sock, int messageSize) throws IOException {
        // Implement for reading chunk
        
        byte [] incomingMess = null;
        RandomAccessFile rafile = null;
        SlaveReadInstructBuffer sribObj = null;

        try {
            InputStream is = sock.getInputStream();
            incomingMess = GFS.readAllData(is, messageSize);
            sribObj = (SlaveReadInstructBuffer) BytesUtil.toObject(incomingMess);
            rafile = new RandomAccessFile(GFS.localPath+"//"+sribObj.chunkName, "r");
            rafile.seek(sribObj.seekVal);
            
            //Update Read instruction buffer to add read data.
            sribObj.chunkData = new byte[sribObj.bytestoRead];
            rafile.read(sribObj.chunkData);
                    
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();    
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if (rafile != null) rafile.close();
          }
        return sribObj;
    }

    private void SaveChunk(Socket sock, int messageSize) throws IOException {

        // TODO Auto-generated method stub
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        FileOutputStream metafos = null;
        BufferedOutputStream metabos = null;

        try {
            //ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
            InputStream is = sock.getInputStream();
            //toRead = messageSize;
/*          while(toRead > 0) {
                int eachRead = toRead > 1024 ? 1024 : toRead;
                byte [] readMessBuff = new byte[eachRead];
                bytesRead = is.read(readMessBuff, 0, eachRead);
                outputStream.write(readMessBuff);
                toRead -= bytesRead;
            }*/
            byte [] incomingMess = GFS.readAllData(is, messageSize);
            System.out.println("Sizes: mess="+messageSize+" incomBuffer ="+incomingMess.length);
            SlavePutInstructBuffer spibObj = (SlavePutInstructBuffer) BytesUtil.toObject(incomingMess);
            fos = new FileOutputStream( GFS.localPath+"//"+spibObj.chunkName);
            bos = new BufferedOutputStream(fos);
            bos.write(spibObj.chunkData, 0 , spibObj.chunksize);
            bos.flush();
            System.out.println("File " + spibObj.chunkName
                    + " downloaded (" + spibObj.chunksize + " bytes read)");
            
            //Save metadata for the incoming file
            String[] splits = spibObj.chunkName.split("_");
            int count = splits.length;
            FileMetaData fmdObj = new FileMetaData(spibObj.filename, 
                                  Integer.parseInt(splits[count-2]), Integer.parseInt(splits[count-1]), spibObj.totalChunks, spibObj.totalFileSize);
            byte[] metaBuff = BytesUtil.toByteArray(fmdObj);
            metafos = new FileOutputStream( GFS.localPath+"//"+spibObj.chunkName+".meta");
            metabos = new BufferedOutputStream(metafos);
            metabos.write(metaBuff, 0 , metaBuff.length);
            metabos.flush();    
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();    
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if (fos != null) fos.close();
            if (bos != null) bos.close();
            if (metafos != null) metafos.close();
            if (metabos != null) metabos.close();
            if (sock != null) sock.close();
          }
    }
}
