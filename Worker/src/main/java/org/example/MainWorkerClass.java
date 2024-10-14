package org.example;

public class MainWorkerClass {
    public static void main(String[] args) {
        Worker worker = new Worker();
        while (true) {
            worker.handleMessage();
            if (worker.isTerminated()) {
                worker.sendTerminated();
                break;
            }
        }
    }
}