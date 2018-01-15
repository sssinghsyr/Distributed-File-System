package os.gfs;

public class MasterMachine {

    public MasterMachine() {
        /* Will create Master Server thread that will listen for new connection from slave machine
         * Receive command transcode from client machine. 
        */
        new MasterServer();
        
        /* Will create Master Heartbeat thread that will listen for HB from slave machines
        */      
        //new MasterHBReceiver();
        
        SlaveNode objSN;
        while(true) {
            for(int machine_id : GFS.configuredSlaveNodes) {
                objSN = GFS.hConnectedSlave.get(machine_id);
                if(objSN == null)
                    System.out.println("Machine :"+machine_id+"- NOT CONNECTED");
                else
                    switch(objSN.status) {
                    case GFS.STATUS_RUNNING:
                        System.out.println("Machine :"+machine_id+"- RUNNING");
                        break;
                    case GFS.STATUS_CONNEDTED:
                        System.out.println("Machine :"+machine_id+"- CONNECTED");
                        break;
                    case GFS.STATUS_DEAD:
                        System.out.println("Machine :"+machine_id+"- DEAD");
                        break;
                    case GFS.STATUS_NO_HB:
                        System.out.println("Machine :"+machine_id+"- NOT RESPONDING");
                        break;
                    case GFS.STATUS_DEAD_AND_REPLICATED:
                        System.out.println("Machine :"+machine_id+"- DEAD AND REPLICATED");
                        break;
                    default:
                        break;
                    }   
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        /*
        try {
            object.mThread.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
         */
    }
}
