package com.sarwesh.aws.dataconsumer;

import java.util.concurrent.CompletableFuture;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

 
/**
 * See https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/kinesis/src/main/java/com/example/kinesis/KinesisStreamEx.java
 * for complete code and more examples.
 */
public class SubscribeToShardSimpleImpl {
 
    //private static final String CONSUMER_ARN = "arn:aws:kinesis:us-east-2:188189771372:stream/devicesimulatorconsumerstream/consumer/javaconsumer:1234567890";
    private static final String SHARD_ID = "shardId-000000000000";
    private static  String CONSUMER_ARN = "arn:aws:kinesis:us-east-2:188189771372:stream/devicesimulatorconsumerstream/consumer/javaconsumer:1558883960";
    
    public static void main(String[] args) {
 
        KinesisAsyncClient client = KinesisAsyncClient.create();
       /* RegisterStreamConsumerRequest  registerStreamConsumerRequest = RegisterStreamConsumerRequest.builder()
        		.consumerName("javaconsumer").streamARN("arn:aws:kinesis:us-east-2:188189771372:stream/devicesimulatorconsumerstream").build();
        CompletableFuture<RegisterStreamConsumerResponse> futureResponse = client.registerStreamConsumer(registerStreamConsumerRequest);
        try {
			RegisterStreamConsumerResponse response = futureResponse.get();
			CONSUMER_ARN = response.consumer().consumerARN();
			System.out.println("Consumer ARN = "+ CONSUMER_ARN);
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}*/
 
        SubscribeToShardRequest request = SubscribeToShardRequest.builder()
                .consumerARN(CONSUMER_ARN)
                .shardId(SHARD_ID)
                .startingPosition(s -> s.type(ShardIteratorType.LATEST)).build();
 
        // Call SubscribeToShard iteratively to renew the subscription periodically.
        while(true) {
            // Wait for the CompletableFuture to complete normally or exceptionally.
            callSubscribeToShardWithVisitor(client, request).join();
        }
 
        // Close the connection before exiting.
        // client.close();
    }
 
 
    /**
     * Subscribes to the stream of events by implementing the SubscribeToShardResponseHandler.Visitor interface.
     */
    private static CompletableFuture<Void> callSubscribeToShardWithVisitor(KinesisAsyncClient client, SubscribeToShardRequest request) {
        SubscribeToShardResponseHandler.Visitor visitor = new SubscribeToShardResponseHandler.Visitor() {
            @Override
            public void visit(SubscribeToShardEvent event) {
                System.out.println("Received subscribe to shard event " + event);
                for(Record r: event.records())
                {
                	SdkBytes data = r.data();
            		System.out.println("Data:" + data.asUtf8String());
                }
            }
        };
        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
                .builder()
                .onError(t -> System.err.println("Error during stream - " + t.getMessage()))
                .subscriber(visitor)
                .build();
        return client.subscribeToShard(request, responseHandler);
    }
}
