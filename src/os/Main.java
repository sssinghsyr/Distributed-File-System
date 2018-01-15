/*---------------------------------------------------------------------------------------*
 *                              Distributed File System                                  *
 *  Main.java - The java entry point for this application                                *
 *  Created by Shashank Singh on 12/01/17.                                               *
 *  ssingh@syr.edu                                                                       *
 *****************************************************************************************/
package os;

import java.io.IOException;

import os.gfs.*;

public class Main {

    public static void main(String args[]) throws IOException {
        // The keep track of the number of arguments
        int argCount;

        // The number arguments
        int argc = args.length;

        // The current argument
        int argv = -1;

        // Process the arguments
        for (argc--, argv++; argc > 0; argc -= argCount, argv += argCount) {

            argCount = 1;

            if (args[argv].compareTo("-gfs") == 0)
            {
                int gfsArgv = 0;
                gfsArgv++;
                
                if(args[argv + gfsArgv].compareTo("-master") == 0) {
                    //Receive file
                    new ReadConfigXml(GFS.MASTER);
                    new MasterMachine();
                }               
                else if(args[argv + gfsArgv].compareTo("-slave") == 0) {
                    gfsArgv++;
                    if (argc != 2) {
                        System.out.println("Error! Wrong input");
                        System.out.println("Ussage: java <ClassName> -gfs -slave <machineId>");
                        System.exit(0);
                    }
                    GFS.id = Integer.parseInt(args[argv+2]);
                    new ReadConfigXml(GFS.SLAVE);
                    new SlaveMachine();

                }               
                else if(args[argv + gfsArgv].compareTo("-client") == 0) {
                    ClientMachine cmObject = null;
                    gfsArgv++;
                    cmObject = new ClientMachine();
                    new ReadConfigXml(GFS.CLIENT);
                    if(args[argv + gfsArgv].compareTo("-put") == 0) {
                        if (argc != 4) {
                            System.out.println("Error! Wrong input");
                            System.out.println("Ussage: java <ClassName> -gfs -client -put <LocalFilePath> <GFS FilePath>");
                            System.exit(0);
                        }
                        gfsArgv++;
                        GFS.LocalFilePath = args[argv + gfsArgv];
                        gfsArgv++;
                        GFS.DestFilePath = args[argv + gfsArgv];
                        cmObject.put();
                    }
                    else if(args[argv + gfsArgv].compareTo("-read") == 0) {
                        if (argc != 5) {
                            System.out.println("Error! Wrong input");
                            System.out.println("Ussage: java <ClassName> -gfs -client -read <GFS FilePath> <seek value> <bytes to read>");
                            System.exit(0);
                        }
                        gfsArgv++;
                        GFS.DestFilePath = args[argv + gfsArgv];
                        gfsArgv++;
                        int seekValue = Integer.parseInt(args[argv + gfsArgv]);
                        gfsArgv++;
                        int byteRead = Integer.parseInt(args[argv + gfsArgv]);
                        cmObject.read(seekValue, byteRead);
                    }
                    else if(args[argv + gfsArgv].compareTo("-fetch") == 0) {
                        if (argc != 3) {
                            System.out.println("Error! Wrong input");
                            System.out.println("Ussage: java <ClassName> -gfs -client -fetch <GFS FilePath>");
                            System.exit(0);
                        }
                        gfsArgv++;
                        GFS.DestFilePath = args[argv + gfsArgv];
                        cmObject.fetch();
                    }
                }
                System.exit(0);
            }

        }

        assert (false);

        return;
    }

}
