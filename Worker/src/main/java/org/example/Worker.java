package org.example;

import software.amazon.awssdk.services.sqs.model.Message;

import java.util.concurrent.atomic.AtomicBoolean;

// import org.json.JSONArray;
import org.json.JSONObject;


public class Worker implements Runnable {
    final static AWS aws = AWS.getInstance();
    sentimentAnalysisHandler sentimentAnalysisHandler = new sentimentAnalysisHandler();
    namedEntityRecognitionHandler namedEntityRecognitionHandler = new namedEntityRecognitionHandler();
    String workerId;
    String manager2workerURL;
    String worker2managerURL;
    boolean terminated;
    //in send message to manager, send the WorkerId
    public Worker(){
        workerId = aws.getInstanceId();
        manager2workerURL = aws.getSqsUrl("manager2worker");
        worker2managerURL = aws.getSqsUrl("worker2manager");
        terminated = false;
    }
    //-----------------GETTERS-----------------
    public String getWorkerId() {
        return workerId;
    }
    public String getManager2workerURL() {
        return manager2workerURL;
    }
    public String getWorker2managerURL() {
        return worker2managerURL;
    }
    public boolean isTerminated() {
        return terminated;
    }
    //-----------------GETTERS-----------------

    //-----------------METHODS-----------------
    //--SQS--
    public Message receiveMessageFromManager(){
        return aws.receiveMessageFromSQS(manager2workerURL);
    }
    public void sendMessageToManager(String msg){
        aws.sendMessageToSQS(worker2managerURL, msg);
    }
    public void deleteMessageFromManager(String msgReciept){
        aws.deleteMessageFromSQS(manager2workerURL, msgReciept);
    }
    public void sendTerminated(){
        sendMessageToManager(buildTerminatedMessage());
    }

    //--runnable--
    public void run() {
        while (true) {
            handleMessage();
            if (isTerminated()) {
                break;
            }
        }
    }

    //--SQS--
    //--Messsage Handling--
    public void handleMessage(){
        Message msg = receiveMessageFromManager();
        if(msg != null){
            String msgBody = msg.body();
            if(msgBody.equals("terminate")){
                terminated = true;
            }
            else{
                JSONObject msgJson = new JSONObject(msgBody);
                String localId = msgJson.getString("localid");
                JSONObject review = msgJson.getJSONObject("review");

                String link = review.getString("link");
                int rating = review.getInt("rating");
                String reviewStr = review.getString("text");

                String entities = namedEntityRecognitionHandler.findEntities(reviewStr);
                int sentiment = sentimentAnalysisHandler.findSentiment(reviewStr);
                boolean sarcasm = sentiment - rating > 2 || sentiment - rating < -2;

                String output = buildMessageToManager(localId, link, sentiment, entities, sarcasm);
                sendMessageToManager(output);
            }
            deleteMessageFromManager(msg.receiptHandle());
        }
    }

    private String buildMessageToManager(String localid, String link, int sentiment, String entities, boolean sarcasm){
        JSONObject msg = new JSONObject();
        JSONObject review = new JSONObject();
        msg.put("type", "output");
        msg.put("localid", localid);
        review.put("link", link);
        review.put("sentiment", sentiment);
        review.put("entities", entities);
        review.put("sarcasm", sarcasm);
        msg.put("review", review.toString());
        return msg.toString();
    }

    private String buildTerminatedMessage(){
        JSONObject msg = new JSONObject();
        msg.put("type", "terminated");
        msg.put("workerId", workerId);
        return msg.toString();
    }
    //--Messsage Handling--

}
