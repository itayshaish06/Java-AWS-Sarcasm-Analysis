package org.example;

import java.io.BufferedReader;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONArray;
import org.json.JSONObject;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import software.amazon.awssdk.services.ec2.model.Ec2Exception;
//import com.amazonaws.services.s3.internal.eventstreaming.Message;
import software.amazon.awssdk.services.sqs.model.Message;


//test
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
public class LocalApp {
    private String[] inputFilePath;
    private String outputFilePath;
    private int tasksPerWorker;
    private boolean terminate;
    private String managerQueueURL;
    private String[] s3JarsUrl;
    private String[] s3fileKeys;
    private String s3OutputkeyString;
    private String localid; //=name of the sqs queue
    private String sqsURl;
    private String managerInstanceID;
    final static AWS aws = AWS.getInstance();

    //------------------Constructor------------------//
    public LocalApp() {
        this.inputFilePath = null;
        this.outputFilePath = "";
        this.tasksPerWorker = 0;
        this.terminate = false;
        this.managerQueueURL = "";
        this.s3JarsUrl = new String[2];
        this.s3fileKeys = null;
        this.s3OutputkeyString = "";
        this.sqsURl = "";
        this.localid = LocalTime.now().toString().replace(":", "_").replace(".","-");
    }
    //------------------Constructor------------------//

    //------------------Getters------------------//
    public String[] getInputFilePath() {
        return inputFilePath;
    }
    public String getOutputFilePath() {
        return outputFilePath;
    }
    public int getTasksPerWorker() {
        return tasksPerWorker;
    }
    public boolean isTerminate() {
        return terminate;
    }
    public String getManagerQueueURL() {
        return managerQueueURL;
    }
    public String[] getS3JarsUrl() {
        return s3JarsUrl;
    }
    public String[] getS3fileKeys() {
        return s3fileKeys;
    }
    public String getS3OutputkeyString() {
        return s3OutputkeyString;
    }
    public String getLocalid() {
        return localid;
    }
    public String getSqsURl() {
        return sqsURl;
    }
    public String getManagerInstanceID() {
        return managerInstanceID;
    }
    public String getSqsName() {
        return localid;
    }
    //------------------Getters------------------//

    //------------------Setters------------------//
    public void setInputFilePath(String[] inputFilePath) {
        this.inputFilePath = inputFilePath;
    }
    public void setOutputFilePath(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }
    public void setTasksPerWorker(int tasksPerWorker) {
        this.tasksPerWorker = tasksPerWorker;
    }
    public void setTerminate(boolean terminate) {
        this.terminate = terminate;
    }
    public void setManagerQueueURL(String managerQueueURL) {
        this.managerQueueURL = managerQueueURL;
    }
    public void setS3JarsUrl(String[] s3JarsUrl) {
        this.s3JarsUrl = s3JarsUrl;
    }
    public void setS3fileKeys(String[] s3fileKeys) {
        this.s3fileKeys = s3fileKeys;
    }
    public void setS3OutputkeyString(String s3OutputkeyString) {
        this.s3OutputkeyString = s3OutputkeyString;
    }
    public void setLocalid(String localid) {
        this.localid = localid;
    }
    public void setSqsURl(String sqsURl) {
        this.sqsURl = sqsURl;
    }
    public void setManagerInstanceID(String managerInstanceID) {
        this.managerInstanceID = managerInstanceID;
    }
    //------------------Setters------------------//

    //------------------Methods------------------//
    //--SQS--//
    public Message receiveMessagesFromSQS() {
        Message msg = aws.receiveMessageFromSQS(sqsURl);
        if(msg!=null){aws.deleteMessageFromSQS(sqsURl, msg.receiptHandle());}
        return msg;
    }

    public Message receiveMessagesFromMangerSQS() {
        App.pritnCurrentTime("[DEBUG] Receiving messages from Manager SQS.");
        Message msg = null;
        while((msg = aws.receiveMessageFromSQS(managerQueueURL)) == null) {
            sleeper(500);
        }
        return msg;
    }
    public void sendMessageToSQS(String msg) {
        App.pritnCurrentTime("[DEBUG] Sending message to SQS.");
        aws.sendMessageToSQS(sqsURl, msg);
    }
    public void sendMessageToMangerSQS(String msg) {
        App.pritnCurrentTime("[DEBUG] Sending message to SQS.");
        aws.sendMessageToSQS(managerQueueURL, msg);
    }
    public void createSQS() {
        App.pritnCurrentTime("[DEBUG] Creating SQS.");
        sqsURl = aws.createSqs(localid,"0");
    }

    //--S3--//
    public void createBucket(){
        if(!aws.checkIfBucketExists(aws.bucketName)){
            aws.createBucketIfNotExists(aws.bucketName);
        }
    }
    public void uploadInputFilesToS3(String[] inputFilePath, int tasksPerWorker, String localid) {
        App.pritnCurrentTime("[DEBUG] Uploading input file to S3.");
        int numOfFiles = inputFilePath.length;
        s3fileKeys = new String[numOfFiles];
        for(int i = 0; i < numOfFiles; i++){
            s3fileKeys[i] = localid + "_input" + i + ".txt";
            aws.uploadFileToS3(aws.bucketName, s3fileKeys[i], inputFilePath[i]);
        }
        //[LIST-URL]-n-LOCALID[TIMESTAMP]
        sendMessageToMangerSQS(buildTaskMessage(s3fileKeys, tasksPerWorker, localid, aws.bucketName));
    }
    private void UploadManagerJarToS3() { //Update path to manager jar
        if(aws.checkIfFileExists("logim", "manager.jar")){
            App.pritnCurrentTime("[DEBUG] Manager JARs already exists in S3.");
            return;
        }
        App.pritnCurrentTime("[DEBUG] Uploading Manager JARs to S3.");
        String filePath = "manager.jar";
        aws.uploadFileToS3("logim", "manager.jar", filePath);
    }
    private void UploadWorkerJarToS3() { //Update path to worker jar
        if(aws.checkIfFileExists("logim", "worker.jar")){
            App.pritnCurrentTime("[DEBUG] Worker JARs already exists in S3.");
            return;
        }
        App.pritnCurrentTime("[DEBUG] Uploading Worker JAR to S3.");
        String filePath = "worker.jar";
        aws.uploadFileToS3("logim", "worker.jar", filePath);
    }

    //--EC2--//
    public void createManagerNodeIfNotExists(){ //Update ami + check how to pass the jars url to the manager node
        // **** check how to pass the sqs url to the manager node ****
        if ((managerInstanceID = aws.searchEC2InstanceByTag("manager"))==null) {
            App.pritnCurrentTime("[DEBUG] Manager node does not exist. Creating manager node.");
            UploadManagerJarToS3();
            UploadWorkerJarToS3();
            managerQueueURL = aws.createSqs("manager");
            managerInstanceID = aws.createEC2Instance("manager", 1);
        } else {
            App.pritnCurrentTime("[DEBUG] Manager node exists.");
            managerQueueURL = aws.getSqsUrl("manager");
        }
    }

    public boolean checkManagerStatus() {
        if (!aws.checkIfEC2InstanceIsRunning(managerInstanceID)) {
            if (terminate) {
                App.pritnCurrentTime("[DEBUG] Manager node died unexpectedly. Creating new manager node.");
                aws.killAllEc2Instances();
                aws.terminateSqs("manager");
                aws.terminateSqs("manager2worker");
                aws.terminateSqs("worker2manager");
                App.pritnCurrentTime("[DEBUG] Waiting 1 min before creating new manager sqs.");
                sleeper(60*1000);
                managerQueueURL = aws.createSqs("manager");
                managerInstanceID = aws.createEC2Instance("manager", 1);
                App.pritnCurrentTime("[DEBUG] New Manager node created.");
                App.pritnCurrentTime("[DEBUG] Uploading.");
                sendMessageToMangerSQS(buildTaskMessage(s3fileKeys, tasksPerWorker, localid, aws.bucketName));
            } else {
                App.pritnCurrentTime("[DEBUG] Manager node died unexpectedly. Waiting 1 min for new manager node to be created.");
                sleeper(60 * 1000);
                if ((managerInstanceID = aws.searchEC2InstanceByTag("manager")) != null) {
                    App.pritnCurrentTime("[DEBUG] New Manager node created.");
                    managerQueueURL = aws.getSqsUrl("manager");
                    sendMessageToMangerSQS(buildTaskMessage(s3fileKeys, tasksPerWorker, localid, aws.bucketName));
                } else {
                    App.pritnCurrentTime("Exiting");
                    return false;
                }
            }
        }
        return true;
    }
    //--Messages--//
    public String buildTaskMessage(String[] inputKeys, int numOfTasksPerWorker, String localid, String bucketName) {
        // Create a JSON object
        JSONObject jsonObject = new JSONObject();

        // Add inputUrls to the JSON array
        JSONArray urlsArray = new JSONArray(inputKeys);
        jsonObject.put("inputKeys", urlsArray);

        jsonObject.put("sqsURL",this.sqsURl);
        // Add numOfWorkers and currentTime to the JSON object
        jsonObject.put("numOfTasksPerWorker", numOfTasksPerWorker);

        // Add localId to the JSON object
        jsonObject.put("localid", localid);

        // Add bucketName to the JSON object
        jsonObject.put("bucketName", bucketName);

        // Convert the JSON object to a string
        return jsonObject.toString();
    }

    private JSONArray buildOutputFromS3(Message msg){
        String s3FileName = msg.body();
        BufferedReader fileReader = aws.downloadFile(aws.bucketName, s3FileName);
        JSONArray outputJasons = null;
        try{
            outputJasons = new JSONArray(fileReader.readLine());
            fileReader.close();
        }catch(Exception e){
            App.pritnCurrentTime(e.getMessage());
        }
        return outputJasons;
    }

    //--HTML--//
    public void createHTMLFileFromReviews(Message results) {
        JSONArray outputJasons = buildOutputFromS3(results);
        if(outputJasons == null){
            App.pritnCurrentTime("No output to create HTML file from");
            return;
        }
        // Create a new file
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath+".html"));
            for(int index = 0; index < outputJasons.length(); index++){
                JSONObject review = outputJasons.getJSONObject(index);
                int sentiment = review.getInt("sentiment");
                String link = review.getString("link");
                String entities = review.getString("entities");
                boolean sarcasm = review.getBoolean("sarcasm");

                // Create HTML elements based on sentiment
                String color;
                switch(sentiment) {
                    case 0: // Very negative
                        color = "darkred";
                        break;
                    case 1: // Negative
                        color = "red";
                        break;
                    case 2: // Neutral
                        color = "black";
                        break;
                    case 3: // Positive
                        color = "lightgreen";
                        break;
                    case 4: // Very positive
                        color = "darkgreen";
                        break;
                    default:
                        color = "black"; // Default to black for unknown sentiment
                }

                // Write HTML elements to file
                writer.write("<p>");
                writer.write("<a href=\"" + link + "\" style=\"color:" + color + "\">Original Review</a><br>");
                writer.write("Named Entities: " + entities + "<br>");
                writer.write("Sarcasm Detected: " + sarcasm + "<br>");
                writer.write("</p>\n");
            }
            writer.close();
        } catch (Exception e) {
            App.pritnCurrentTime(e.getMessage());
        }
    }

    //--Terminate--//
    public boolean terminate() {
        try {
            sleeper(1000);
            Message msg = null;
            do {
                msg = receiveMessagesFromSQS();
            } while (msg!=null && !msg.body().equals("terminated"));
            aws.terminateInstance(managerInstanceID);
            aws.terminateSqs("manager");
            aws.terminateSqs(getSqsName());
            aws.closeResources(true);//delete files from bucket and close s3, ec2, sqs clients

            return true;
        } catch (Ec2Exception e) {
            App.pritnCurrentTime(e.getMessage());
        }
        return false;
    }


    public void sendTerminateIfNecessary() {
        if (terminate) {
            App.pritnCurrentTime("[DEBUG] Sending terminate message.");
            sendMessageToSQS("terminate");
        }
    }

    public void terminateIfNecessary() {
        if (terminate) {
            App.pritnCurrentTime("[DEBUG] Terminating.");
            if(!terminate()) {
                App.pritnCurrentTime("[DEBUG] Failed to terminate.");
            }
        }
        else{
            App.pritnCurrentTime("[DEBUG] Closing Local resources.");
            closeLocalSqsAndClients();
        }
    }
    public void closeLocalSqsAndClients() {
        aws.terminateSqs(getSqsName());
        aws.closeResources(false);
    }
    //------------------Methods------------------//

    public static void sleeper(int miliseconds) {
        try {
            Thread.sleep(miliseconds);
        } catch (InterruptedException e) {
            App.pritnCurrentTime(e.getMessage());
        }
    }

    public static void pritnCurrentTime(String msg){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        App.pritnCurrentTime(sdf.format(new Date()) + "\n" + msg+"\n");
    }
}

