package org.example;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import software.amazon.awssdk.services.sqs.model.Message;


public class App {
    final static AWS aws = AWS.getInstance();
    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)]
        LocalApp localApp = new LocalApp();
        manageArgs(localApp, args);
        try {
            setup(localApp);
        } catch (Exception e) {
            e.printStackTrace();
        }
        pritnCurrentTime("[DEBUG] Waiting for manager node to be finish working on the input file.");
        //------------------Wait for manager to finish------------------//

        pritnCurrentTime("[DEBUG] Receiving messages from SQS.");
        Message results = null;
        boolean managerRunning = true;
        while(managerRunning && results == null){
            managerRunning = localApp.checkManagerStatus();
            for(int i = 0; i < 3 && managerRunning && results == null; i++)
                results = localApp.receiveMessagesFromSQS();
        }
        if(managerRunning)
            pritnCurrentTime("[DEBUG] Received message.");

        if(!managerRunning || results.body().equals("terminated")) { //if manager terminated by other localApp
            pritnCurrentTime("[DEBUG] Manager terminated.");
            localApp.closeLocalSqsAndClients();
        }
        else{
            pritnCurrentTime("[DEBUG] Checking if I need to send Terminate.");
            localApp.sendTerminateIfNecessary();
            pritnCurrentTime("[DEBUG] Creating HTML file.");
            localApp.createHTMLFileFromReviews(results);
            localApp.terminateIfNecessary();
        }
    }

    //Create Buckets, Create Queues, Upload JARs to S3
    private static void setup(LocalApp localApp) {
        pritnCurrentTime("[DEBUG] Create bucket if not exist.");
        localApp.createBucket();
        pritnCurrentTime("[DEBUG] Check if manager node exists.");
        localApp.createManagerNodeIfNotExists(); //jars are uploaded to s3 in this function
        pritnCurrentTime("[DEBUG] Create queue.");
        localApp.createSQS();
        pritnCurrentTime("[DEBUG] Upload input files to S3.");
        localApp.uploadInputFilesToS3(localApp.getInputFilePath(), localApp.getTasksPerWorker(), localApp.getLocalid());
    }

    private static void manageArgs(LocalApp localApp, String[] args) {
        int argsLength = args.length;
        if(args[argsLength-1].equals("-t")) {
            localApp.setTerminate(true);
            argsLength--;
            pritnCurrentTime("[DEBUG] Terminate flag is on.");
        }
        try {
            localApp.setTasksPerWorker(Integer.parseInt(args[argsLength-1]));
            argsLength--;
            pritnCurrentTime("[DEBUG] Tasks per worker: " + localApp.getTasksPerWorker());
        }
        catch (Exception e){
            System.err.println("Error: tasksPerWorker must be a number.");
            System.exit(1);
        }
        localApp.setOutputFilePath(args[argsLength-1]);
        argsLength--;
        pritnCurrentTime("[DEBUG] Output file path: " + localApp.getOutputFilePath());
        String[] inputFilePath = new String[argsLength];
        for (int i = argsLength-1; i>=0;i--){
            inputFilePath[i] = args[i];
            pritnCurrentTime("[DEBUG] Input file path "+i+": " + inputFilePath[i]);
        }
        localApp.setInputFilePath(inputFilePath);
    }

    public static void pritnCurrentTime(String msg){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        System.out.println(sdf.format(new Date()) + " - " + msg+"\n");
    }

}

