package os.gfs;

public class SlaveNode {
    int id;
    String address; // of form "ipaddress:port"
    int status;
    long lastHBReceived;
    
    public SlaveNode(int iid, String iaddr, int istatus, long itime) {
        id = iid;
        address = iaddr;
        status = istatus;
        lastHBReceived = itime;
    }
}