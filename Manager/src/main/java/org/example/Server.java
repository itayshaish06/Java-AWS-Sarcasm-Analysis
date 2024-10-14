package org.example;

// import java.io.BufferedReader;
// import java.io.FileReader;
// import java.io.IOException;

// import org.json.JSONArray;
// import org.json.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Server {
    public static void sleeper(int miliseconds){
        try{
            Thread.sleep(miliseconds);
        }
        catch(InterruptedException e){
            System.out.println(e.getMessage());
        }
    }

    public static void workerThread(Manager manager){
        pritnCurrentTime("[Worker Thread] - workerThread started");
        while(!manager.isTerminatefromLocal()){
            for (int i = 0; i < 20 && !manager.isTerminatefromLocal(); i++) {
                manager.handleWorkerMessage();
            }
            manager.checkWorkersStatus();
        }

        while(manager.getNumOfWorkers() > 0){
            manager.handleWorkerMessage();
        }
        pritnCurrentTime("[Worker Thread] workerThread terminated");
    }

    public static void waitForWorkerThread(Thread workerThread){
        try{
            workerThread.join();
        }
        catch(InterruptedException e){
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {
        Manager manager = new Manager();
        Thread workerThread = new Thread(()->{workerThread(manager);}, "workerThread");
        workerThread.start();
        pritnCurrentTime("[Main Thread] - Entering main loop");
        while (true) {
            manager.handleConnectionMessage();
            manager.handleTerminate();
            if(manager.isTerminatefromLocal()){
                while(manager.getNumOfMessagesInManger2WorkerQueue() > 0){
                    sleeper(1000);
                }
                break;
            }
        }
        pritnCurrentTime("[Main Thread] - Exiting main loop and sending terminate message to workers");
        manager.sendTerminateMessageToWorkers(manager.getNumOfWorkers());
        pritnCurrentTime("[Main Thread] - Waiting for worker thread to terminate");
        waitForWorkerThread(workerThread);
        pritnCurrentTime("[Main Thread] - Worker thread terminated");
        manager.terminate();
    }

    public static void pritnCurrentTime(String msg){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        System.out.println(sdf.format(new Date()) + "\n" + msg+"\n");
    }

}