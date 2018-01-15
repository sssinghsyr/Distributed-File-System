package os.gfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;


public class SlaveHBSender implements Runnable {

    public Thread mThread;

    public SlaveHBSender() {
        mThread = new Thread(this);
        mThread.start();
    }
    @Override
    public void run(){
        DatagramSocket clientSocket = null;
        BufferedReader inFromUser =
                new BufferedReader(new InputStreamReader(System.in));
        try {
            clientSocket = new DatagramSocket();
            InetAddress IPAddress = InetAddress.getByName("localhost");
            byte[] sendData = new byte[1024];
            byte[] receiveData = new byte[1024];
            String sentence = inFromUser.readLine();
            sendData = sentence.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, 9876);
            clientSocket.send(sendPacket);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(receivePacket);
            String modifiedSentence = new String(receivePacket.getData());
            System.out.println("FROM SERVER:" + modifiedSentence);
            clientSocket.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if(clientSocket != null) clientSocket.close();
        }
    }

}
