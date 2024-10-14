package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// import com.amazonaws.services.s3.AmazonS3Client;
// import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }
    public void closeResources() {
        deleteObjectsInBucket(bucketName);
        s3.close();
        sqs.close();
        ec2.close();
    }

    public String bucketName = "danielitaybucket";


    //------------------S3-------------------//
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void uploadFileToS3(String bucketName, String key, String filePath) {
        try {
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    RequestBody.fromBytes(Files.readAllBytes(Paths.get(filePath))));
        } catch (S3Exception | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public String getFileUrl(String bucketName, String key) {
        try {
            return s3.utilities().getUrl(GetUrlRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build()).toExternalForm();
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public void deleteObjectsInBucket(String bucket) {
        try {
            // To delete a bucket, all the objects in the bucket must be deleted first.
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(s3Object.key())
                            .build();
                    s3.deleteObject(request);
                }
            } while (listObjectsV2Response.isTruncated());
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
            s3.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());

        }
    }

    public BufferedReader downloadFile(String bucketName, String key) {
        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
            return new BufferedReader(new InputStreamReader(objectBytes.asInputStream()));
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    //------------------S3-------------------//



    //------------------SQS------------------//
    public String createSqs(String queueName) {
        try {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
            return createQueueResponse.queueUrl();
        } catch (SqsException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public String createSqs(String queueName, String timeout) {
        try {
            Map<QueueAttributeName, String> attributes = new HashMap<QueueAttributeName, String>();
            attributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, timeout);            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(attributes)
                    .build();
            CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
            return createQueueResponse.queueUrl();
        } catch (SqsException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public void sendMessageToSQS(String sqsURL, String message) {
        try {
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(sqsURL)
                    .messageBody(message)
                    .build());
        } catch (SqsException e) {
            System.out.println(e.getMessage());
        }
    }

    public Message receiveMessageFromSQS(String sqsURL) {
        try {
            ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(sqsURL)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20)
                    .build());
            if (response.hasMessages())
                return response.messages().get(0);
        } catch (SqsException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public void deleteMessageFromSQS(String sqsURL, String receiptHandle) {
        try {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(sqsURL)
                    .receiptHandle(receiptHandle)
                    .build());
        } catch (SqsException e) {
            System.out.println(e.getMessage());
        }
    }

    public void terminateSqs(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public String getSqsUrl(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            GetQueueUrlResponse getQueueResponse = sqs.getQueueUrl(getQueueRequest);
            return getQueueResponse.queueUrl();
        } catch (SqsException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }
    //------------------SQS------------------//


    //------------------EC2------------------//
    public String createEC2Instance(String tagName, String amiId) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T1_MICRO)
                .maxCount(1)
                .minCount(1)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.println("Successfully started EC2 Instance " + instanceId +" based on AMI " + amiId);
            return instanceId;

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return "";
    }

    public boolean checkIfEC2InstanceIsRunning(String instanceId) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        try {
            DescribeInstancesResponse response = ec2.describeInstances(request);
            InstanceStateName state = response.reservations().get(0).instances().get(0).state().name();
            return state.equals(InstanceStateName.RUNNING);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return false;
    }

    public String searchEC2InstanceByTag(String searchTag) {
        try {
            DescribeInstancesResponse response = ec2.describeInstances();
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    for (Tag tag : instance.tags()) {
                        if (tag.value().equals(searchTag)) {
                            return instance.instanceId();
                        }
                    }
                }
            }
        } catch (Ec2Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public void terminateInstance(String instanceID) {
        try {
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceID)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();
            for (InstanceStateChange sc : list) {
                System.out.println("The ID of the terminated instance is " + sc.instanceId());
            }

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public DescribeInstancesResponse describeInstances() {
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .build();
            return ec2.describeInstances(request);
        } catch (Ec2Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public String getInstanceId() {
        return EC2MetadataUtils.getInstanceId();
    }
    //------------------EC2------------------//

    public static void sleeper(int miliseconds) {
        try {
            Thread.sleep(miliseconds);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}
