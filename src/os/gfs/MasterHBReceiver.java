package os.gfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;


public class MasterHBReceiver implements Runnable {

    public Thread mThread;
    private int machine_id;
    Socket isock;

    public MasterHBReceiver(int id, Socket sock) {
        machine_id = id;
        isock = sock;
        mThread = new Thread(this);
        mThread.start();
    }
    @Override
    public void run(){
        HeartBeatMessage hbObj = null;
        int bytesRead;
        try {
            InputStream is = isock.getInputStream();
            //isock.setSoTimeout(1000*GFS.HeartBeatIntrv);
            isock.setKeepAlive(true);
            while(true) {
                byte [] heartbeatBuff = new byte[GFS.HB_MESS_SIZE];
                bytesRead = is.read(heartbeatBuff, 0, heartbeatBuff.length);
                try {
                    hbObj = (HeartBeatMessage) BytesUtil.toObject(heartbeatBuff);
                } catch (ClassNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                System.out.println("HeartBeat received from machine id ="+hbObj.machine_id);
                if(GFS.alRunningSlave.contains(machine_id) == false) {
                    GFS.alRunningSlave.add(machine_id);
                    SlaveNode objSN = GFS.hConnectedSlave.get(machine_id);
                    objSN.status = GFS.STATUS_RUNNING;
                }
            }
        }catch (SocketTimeoutException e) {
            System.out.println("Timeout triggered from machine_id ="+machine_id);
        } catch (SocketException e) {
            System.out.println("Connection reset by "+machine_id);
            SlaveNode objSN = GFS.hConnectedSlave.get(machine_id);
            objSN.status = GFS.STATUS_NO_HB;
            int idx = GFS.alRunningSlave.indexOf(machine_id);
            GFS.alRunningSlave.remove(idx);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if(isock != null)
                try {
                    isock.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
    }

}
