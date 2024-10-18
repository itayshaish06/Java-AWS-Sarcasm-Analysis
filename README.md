# Assignment 1: Sarcasm Analysis

## Project Description

The goal of this project is to learn how to work with java SDK, AWS s3, AWS sqs, AWS ec2.
The Assignment analyze the sarcasm in a given text.

## Project Flow

1. The user starts the local application which uploads 5 files of reviwes to be analyzed to the s3 bucket.
2. The application sends a message to the sqs queue with the file name.
3. The application does one of the following:
    * Starts the manager.
    * Checks if a manager is active and if not, starts it.
4. The manager reads the message from the sqs, downloads the file from the s3 bucket, parses the files and sends the messages to the workers.
5. The manager start the workers. (according to the number of reviews per worker that the user chose).
6. The workers read the messages from the sqs, analyze the reviews and send the results to the results queue.
7. The manager reads the results from the results queue and writes the results to a file in the s3 bucket.
8. The manager sends the user a message with the file name.
9. The application downloads the file from the s3 bucket, creating html file with the results.
10. The application terminate the manager and the workers if the user chose to do so.

    ![image](https://github.com/user-attachments/assets/40ce211d-beed-4fb6-9ecf-ddfad1931f93)


## Running the Project

1. On:
   * Windoes `->` run `runMe.bat` file. To clean `->` run `clean.bat`
   * Linux `->` run `Terminal` anf run `runMeLinux.sh` file. To clean `->` run `./cleanLinux.sh`.
2. Make sure you save your AWS credentials in the `credentials` file.
3. Run the `Local` jar by entering `jars` directory and running the following command:
    * With Terminate:
   ```bash
   java -jar Local.jar inputs/input1.txt inputs/input2.txt inputs/input3.txt inputs/input4.txt inputs/input5.txt out <number of reviews per worker> -t
   ```
   For example:
   ```bash
   java -jar Local.jar inputs/input1.txt inputs/input2.txt inputs/input3.txt inputs/input4.txt inputs/input5.txt out 320 -t
   ```
   * Without Terminate:
   ```bash
   java -jar Local.jar inputs/input1.txt inputs/input2.txt inputs/input3.txt inputs/input4.txt inputs/input5.txt out <number of reviews per worker>
   ```
   For example:
   ```bash
   java -jar Local.jar inputs/input1.txt inputs/input2.txt inputs/input3.txt inputs/input4.txt inputs/input5.txt out 320
   ```

## Scalability
* Maintaining locals in parallel:
  * Each Local sends its request to the manager SQS, and opens a new SQS for the results.
  In that way we can maintain the locals in parallel, without any local trying to access another local results.
* Maintaining work inside the manager:
   * The manager is devided to 2 threads:
     * One for reading the requests from Locals and distribute the Work to the Workeres.
     * One for reading the results from Workers, combine it to 1 message per Local and send it back.
   With that we can maintain the work inside the manager in parallel, without the program being stuck on one Local task.
* Workers management:
  * Under AWS accademy restrictions - we do not create more than 8 workers at the same time (due to 9 instances limit).
  * The manager is monitoring the number of workers according to the current task requirements.
    * When the manager parses an new task - it will create new workers, according to task requirement.
    * When the manager finished working on a task, it terminates workers, according to task requirement.

## Persistence
* Unexpected Termination:
  * In case of unexpected termination of a worker - The manager creates a new one.
   the Message task will not be lost due to "visibuilty timeout" (Worker deletes a message only after it finishes the task and sends the result to the results queue). 
  * In case of unexpected termination of the manager - The local with the terminate flag will terminate the workers and the manager, and restart the whole process.
