package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.sqs.model.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalTime;

public class Manager {
    private static final int MAX_INSTANCES = 8;
    final static AWS aws = AWS.getInstance();
    private ConcurrentHashMap<String, String> sqsLocalURL;
    private ConcurrentHashMap<String, Integer> numOfWorkersPerLocal;
    private ConcurrentHashMap<String, Integer> numOfReviewsPerLocal;
    private ConcurrentHashMap<String, JSONArray> localid2OutputMessage;
    private ConcurrentSkipListSet<String> workerID;
    private String manager2WorkerURLSqs;
    private String worker2ManagerURLSqs;
    private String managerSqsURL;
    private AtomicInteger activeWorkers;
    private volatile boolean terminatefromLocal;

    //------------------Constructor------------------//
    public Manager() {
        sqsLocalURL = new ConcurrentHashMap<String, String>();
        numOfWorkersPerLocal = new ConcurrentHashMap<String, Integer>();
        manager2WorkerURLSqs = aws.createSqs("manager2worker","300"); // 300 is the visibility timeout
        worker2ManagerURLSqs = aws.createSqs("worker2manager");
        managerSqsURL = getSqsUrl("manager");
        activeWorkers = new AtomicInteger(0);
        terminatefromLocal = false;
        numOfReviewsPerLocal = new ConcurrentHashMap<String, Integer>();
        localid2OutputMessage = new ConcurrentHashMap<String, JSONArray>();
        workerID = new ConcurrentSkipListSet<String>();
    }
    //------------------Constructor------------------//

    //------------------Getters------------------//
    public ConcurrentHashMap<String, String> getSqsLocalURL() {
        return sqsLocalURL;
    }
    public ConcurrentHashMap<String, Integer> getNumOfWorkersPerLocal() {
        return numOfWorkersPerLocal;
    }
    public ConcurrentHashMap<String, Integer> getNumOfReviewsPerLocal() {
        return numOfReviewsPerLocal;
    }
    public ConcurrentHashMap<String, JSONArray> getLocalid2OutputMessage() {
        return localid2OutputMessage;
    }
    public ConcurrentSkipListSet<String> getWorkerID() {
        return workerID;
    }
    public String getManager2WorkerURLSqs() {
        return manager2WorkerURLSqs;
    }
    public String getWorker2ManagerURLSqs() {
        return worker2ManagerURLSqs;
    }
    public String getManagerSqsURL() {
        return managerSqsURL;
    }
    public int getActiveWorkers() {
        return activeWorkers.get();
    }
    public boolean isTerminatefromLocal() {
        return terminatefromLocal;
    }
    //------------------Getters------------------//

    //------------------Setters------------------//
    public void setSqsLocalURL(ConcurrentHashMap<String, String> sqsLocalURL) {
        this.sqsLocalURL = sqsLocalURL;
    }
    public void setNumOfWorkersPerLocal(ConcurrentHashMap<String, Integer> numOfWorkersPerLocal) {
        this.numOfWorkersPerLocal = numOfWorkersPerLocal;
    }
    public void setNumOfReviewsPerLocal(ConcurrentHashMap<String, Integer> numOfReviewsPerLocal) {
        this.numOfReviewsPerLocal = numOfReviewsPerLocal;
    }
    public void setLocalid2OutputMessage(ConcurrentHashMap<String, JSONArray> localid2OutputMessage) {
        this.localid2OutputMessage = localid2OutputMessage;
    }
    public void setWorkerID(ConcurrentSkipListSet<String> workerID) {
        this.workerID = workerID;
    }
    public void setManager2WorkerURLSqs(String manager2WorkerURLSqs) {
        this.manager2WorkerURLSqs = manager2WorkerURLSqs;
    }
    public void setWorker2ManagerURLSqs(String worker2ManagerURLSqs) {
        this.worker2ManagerURLSqs = worker2ManagerURLSqs;
    }
    public void setManagerSqsURL(String managerSqsURL) {
        this.managerSqsURL = managerSqsURL;
    }
    public void setActiveWorkers(AtomicInteger activeWorkers) {
        this.activeWorkers = activeWorkers;
    }
    public void setTerminatefromLocal(boolean terminatefromLocal) {
        this.terminatefromLocal = terminatefromLocal;
    }
    //------------------Setters------------------//

    //------------------Methods------------------//
    //--EC2--//
    public void createWorkers(int numOfInstances) {
        activeWorkers.addAndGet(numOfInstances);
        String[] tagName = new String[numOfInstances];
        for (int i = 0; i < numOfInstances; i++) {
            tagName[i] = ""+LocalTime.now()+i;
        }
        String[] instancesID = aws.createEC2Instance(tagName, numOfInstances);
        for(String instanceID : instancesID){
            workerID.add(instanceID);
        }
    }
    public int getNumOfWorkers(){//FROM AWS SERVICE
        int counter = 0;
        DescribeInstancesResponse ec2DescribeInstances = aws.describeInstances();
        for (Reservation reservation : ec2DescribeInstances.reservations()) {
            for (Instance instance : reservation.instances()) {
                if(instance.state().name().toString().equals("running")){
                    for(Tag tag : instance.tags()){
                        if(tag.key().equals("Name") && !tag.value().equals("manager")){
                            counter++;
                        }
                    }
                }
            }
        }
        return counter;
    }
    public void terminateWorker(String workerID) {
        aws.terminateInstance(workerID);
    }
    public void checkWorkersStatus() {
        if(!terminatefromLocal)
            for(String instanceID : workerID){
                if(!aws.checkIfEC2InstanceIsRunning(instanceID)){
                    if(terminatefromLocal)
                        break;
                    terminateWorker(instanceID);
                    workerID.remove(instanceID);
                    activeWorkers.decrementAndGet();
                    createWorkers(1);
                }
            }
    }
    //--SQS--//
    public String getSqsUrl(String sqsName){
        return aws.getSqsUrl(sqsName);
    }
    public void sendMessageToWorker(String message) {
        aws.sendMessageToSQS(manager2WorkerURLSqs, message);
    }
    public void sendMessageToLocal(String message, String localId) {
        aws.sendMessageToSQS(sqsLocalURL.get(localId), message);
    }
    private Message receiveMessageFromWorker(){
        return aws.receiveMessageFromSQS(worker2ManagerURLSqs);
    }
    private Message receiveMessageFromLocal(String localSqsURL){
        return aws.receiveMessageFromSQS(localSqsURL);
    }
    private Message receiveConnectionMessage(){
        return aws.receiveMessageFromSQS(managerSqsURL);
    }
    public void sendTerminateMessageToWorkers(int numOfInstances) {
        for(int messageNum = 0; messageNum < numOfInstances; messageNum++){
            sendMessageToWorker("terminate");
        }
    }
    public int getNumOfMessagesInManger2WorkerQueue(){
        return aws.getNumberOfMessagesInQueue(manager2WorkerURLSqs);
    }
    //--S3--//
    public String UploadFileFromString (String msg, String localId) {
        try {
            String fileName = localId + ".txt";
            Files.write(Paths.get(fileName), msg.getBytes());
            aws.uploadFileToS3(aws.bucketName, fileName, fileName);
            Files.delete(Paths.get(fileName));
            return fileName;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    //--HashMaps--//
    public void addLocal(String localId, String localURL) {
        sqsLocalURL.put(localId, localURL);
    }
    public void removeLocal(String localId) {
        sqsLocalURL.remove(localId);
    }
    //--Message Handling--//
    public boolean handleWorkerMessage() {
        Message msg = receiveMessageFromWorker();
        if (msg != null) {
            JSONObject jsonMessage = new JSONObject(msg.body());
            if (jsonMessage.getString("type").equals("terminated")) {
                String instanceID = jsonMessage.getString("workerId");
                terminateWorker(instanceID);
                workerID.remove(instanceID);
                aws.deleteMessageFromSQS(worker2ManagerURLSqs, msg.receiptHandle());
                activeWorkers.decrementAndGet();
                return true;
            } else {
                String localid = jsonMessage.getString("localid");
                String reviewOutput = jsonMessage.getString("review");
                combineOutputMessages(localid, reviewOutput);
            }
            aws.deleteMessageFromSQS(worker2ManagerURLSqs, msg.receiptHandle());
        }
        return false;
    }

    public void handleTerminate() {
        for(String sqsURL : sqsLocalURL.values()){
            Message msg = receiveMessageFromLocal(sqsURL);
            if(msg != null){
                if(msg.body().equals("terminate")){
                    terminatefromLocal = true;
                    aws.deleteMessageFromSQS(sqsURL, msg.receiptHandle());
                }
                // else{
                //     aws.noVisibuiltyTimeOut(msg.receiptHandle(),sqsURL);
                //     Server.pritnCurrentTime(msg.body() + " noVisibuiltyTimeOut ");
                // }
            }
        }
    }

    public void handleConnectionMessage() {
        Message msg = receiveConnectionMessage();
        if (msg != null) {
            Server.pritnCurrentTime("Starting to parse input files");
            JSONObject jsonMessage = new JSONObject(msg.body());
            String localid = jsonMessage.getString("localid");
            String localURL = jsonMessage.getString("sqsURL");
            sqsLocalURL.put(localid, localURL);
            JSONArray inputKeys = jsonMessage.getJSONArray("inputKeys");
            int numOfTasksPerWorker = jsonMessage.getInt("numOfTasksPerWorker");
            String bucketName = jsonMessage.getString("bucketName");
            localid2OutputMessage.put(localid, new JSONArray());
            int numOfReviews = parseInputFiles(inputKeys, localid, bucketName);
            numOfReviewsPerLocal.put(localid, numOfReviews);
            int neededWorkers = numOfReviews / numOfTasksPerWorker;
            numOfWorkersPerLocal.put(localid, neededWorkers);
            if(neededWorkers + activeWorkers.get() < MAX_INSTANCES){createWorkers(neededWorkers);}
            else{createWorkers(MAX_INSTANCES - activeWorkers.get() - 1);}
            aws.deleteMessageFromSQS(managerSqsURL, msg.receiptHandle());
            Server.pritnCurrentTime("Finished parsing input files");
        }
    }
    //--Parsing Input Files--//
    public int parseInputFiles(JSONArray inputKeys, String localid, String bucketName) {
        int numOfReviews = 0;
        String[] inputKeysArray = convertJSONArrayToString(inputKeys);
        for(String inputKey : inputKeysArray){
            try{
                BufferedReader fileBytes = aws.downloadFile(bucketName, inputKey);
                String line;
                while((line = fileBytes.readLine()) != null){
                    JSONObject jsonLine = new JSONObject(line);
                    String title = jsonLine.getString("title");
                    JSONArray reviews = jsonLine.getJSONArray("reviews");
                    numOfReviews += reviews.length();
                    for(int i = 0; i < reviews.length(); i++){
                        sendMessageToWorker(buildTaskForWorker(localid, reviews.getJSONObject(i)));
                    }
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
        return numOfReviews;
    }
    public String[] convertJSONArrayToString(JSONArray inputKeys) {
        String[] inputKeysArray = new String[inputKeys.length()];
        for (int i = 0; i < inputKeys.length(); i++) {
            inputKeysArray[i] = inputKeys.getString(i);
        }
        return inputKeysArray;
    }
    public String buildTaskForWorker(String localid, JSONObject review) {
        JSONObject message = new JSONObject();
        message.put("localid", localid);
        message.put("review", review);
        return message.toString();
    }
    //--General--//
    public boolean reduceNumOfReviews(String localid){
        int newNumOfReviews = numOfReviewsPerLocal.get(localid) - 1;
        numOfReviewsPerLocal.put(localid,newNumOfReviews);
        int numOfReviewsRead = 2440 - newNumOfReviews;
        if(numOfReviewsRead % 100 == 0)
            Server.pritnCurrentTime("NumOfReviews read " + numOfReviewsRead);
        return newNumOfReviews == 0;
    }
    public void updateNumOfReviews(String localid, int updatedNumOfReviws){
        numOfReviewsPerLocal.put(localid, updatedNumOfReviws);
    }
    //--Termination--//
    public void terminate(){
        for(String instanceID : workerID){
            terminateWorker(instanceID);
        }
        aws.terminateSqs("manager2worker");
        aws.terminateSqs("worker2manager");
        for(String localSqsName : sqsLocalURL.keySet()){
            sendMessageToLocal("terminated", localSqsName);
        }
    }
    //--reducer of workers--//
    public void combineOutputMessages(String localid, String MessageFromWorker){
        JSONObject jsonOutputMessage = new JSONObject(MessageFromWorker);
        JSONArray outputMessage = localid2OutputMessage.get(localid);
        outputMessage.put(jsonOutputMessage);
        if(reduceNumOfReviews(localid)){ //checks if the task is over
            String fileNameInS3 = UploadFileFromString(outputMessage.toString(), localid);
            //send output message to local
            sendMessageToLocal(fileNameInS3, localid);
            //removes the localid from the hashmap
            localid2OutputMessage.remove(localid);
            numOfWorkersPerLocal.remove(localid);
            int neededWorkers = 0;
            for(int numOfWorkers: numOfWorkersPerLocal.values()){
                neededWorkers += numOfWorkers;
            }
            if(neededWorkers < activeWorkers.get()){
                int numOfWorkersToKill = activeWorkers.get() - neededWorkers;
                for(int i = 0; i < numOfWorkersToKill; i++)
                    sendMessageToWorker("terminate");
            }
        }
        else {
            localid2OutputMessage.put(localid, outputMessage);
        } // saves the updated output message
    }

    //------------------Methods------------------//

}

