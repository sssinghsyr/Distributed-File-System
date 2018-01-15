package os.gfs;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;

public class ReadConfigXml {

    public ReadConfigXml() {}

    public ReadConfigXml(int type) {

        try {
            //System.out.println("Current relative path is: " + Paths.get(".").toAbsolutePath().normalize().toString());
            File fXmlFile = null;
            fXmlFile = new File("config.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);

            doc.getDocumentElement().normalize();

            NodeList nList = doc.getElementsByTagName("Machine");

    for (int temp = 0; temp < nList.getLength(); temp++) {

                Node nNode = nList.item(temp);

                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    int machine_id = Integer.parseInt(eElement.getAttribute("id"));
                    if(machine_id > 1000 && machine_id < 2000) {
                        GFS.MasterIP = eElement.getElementsByTagName("IPAddress").item(0).getTextContent();
                        GFS.MasterPort = Integer.parseInt(eElement.getElementsByTagName("Port").item(0).getTextContent());
                        GFS.HeartBeatIntrv = Integer.parseInt(eElement.getElementsByTagName("HeartBeatInterval").item(0).getTextContent());
                        GFS.chunkSize = Integer.parseInt(eElement.getElementsByTagName("ChunkSize").item(0).getTextContent()) * 1024;
                        GFS.replicationFact = Integer.parseInt(eElement.getElementsByTagName("ReplicationFactor").item(0).getTextContent());
                    }
                    if(type == GFS.MASTER && machine_id > 2000 && machine_id < 3000) {
                        //Record all slave machines
                        if(GFS.configuredSlaveNodes.contains(machine_id))
                            System.out.println("Multiple entry in config file for machine_id="+machine_id);
                        else
                            GFS.configuredSlaveNodes.addLast(machine_id);
                    }
                    if(type == GFS.SLAVE && machine_id == GFS.id){
                        GFS.SlaveIP = eElement.getElementsByTagName("IPAddress").item(0).getTextContent();
                        GFS.SlavePort = Integer.parseInt(eElement.getElementsByTagName("Port").item(0).getTextContent());
                        GFS.localPath = eElement.getElementsByTagName("LocalData").item(0).getTextContent();
                    }
                    if(type == GFS.CLIENT && machine_id > 2000 && machine_id < 3000 ){
                        String ip = eElement.getElementsByTagName("IPAddress").item(0).getTextContent();
                        int port = Integer.parseInt(eElement.getElementsByTagName("Port").item(0).getTextContent());
                        SlaveAddress saObj = new SlaveAddress(ip, port);
                        GFS.htSlaveTable.put(machine_id, saObj);
                    }
                    if(type == GFS.CLIENT && machine_id > 3000 && machine_id < 4000 ){
                        GFS.localPath = eElement.getElementsByTagName("LocalData").item(0).getTextContent();
                    }
                }
            }
        }catch (FileNotFoundException e1) {
            System.out.println("Error! Cannot find config file.");
            System.out.println("Absolute file path required: "+Paths.get(".").toAbsolutePath().normalize().toString()+"\\config.xml");
            System.exit(0);
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static String getAddressForNode(int id) {
        try {
            File fXmlFile = new File("C:\\Users\\sssin\\eclipse-workspace\\GFS_Data\\config\\config.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);

            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            NodeList nList = doc.getElementsByTagName("Machine");

            for (int temp = 0; temp < nList.getLength(); temp++) {

                Node nNode = nList.item(temp);

                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    int machine_id = Integer.parseInt(eElement.getAttribute("id"));
                    if(machine_id == id) {
                        String ipaddr = eElement.getElementsByTagName("IPAddress").item(0).getTextContent();
                        String port = eElement.getElementsByTagName("Port").item(0).getTextContent();
                        return ipaddr+":"+port;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}