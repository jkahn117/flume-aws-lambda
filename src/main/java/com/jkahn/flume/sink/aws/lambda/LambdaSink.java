package com.jkahn.flume.sink.aws.lambda;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;

/**
 * Apache Flume sink for AWS Lambda.
 * 
 * {@code
 * a1.sources = r1
 * a1.sinks = k1
 * a1.channel = c1
 * 
 * ...
 * 
 * a1.sinks.k1.type = com.jkahn.flume.sink.aws.lambda.LambdaSink
 * a1.sinks.k1.channel = c1
 * a1.sinks.k1.region = us-east-1
 * a1.sinks.k1.functionName = myFunctionName
 * # credentials - if not specified, will use {@link DefaultAWSCredentialsProviderChain}
 * a1.sinks.k1.accessKey = ...
 * a1.sinks.k1.secretKey = ...
 * 
 * }
 * 
 * 
 * @author jkahn
 *
 */
public class LambdaSink extends AbstractSink implements Configurable {

	private static final Log LOG = LogFactory.getLog(LambdaSink.class);
	
	// Flume details
	private SinkCounter sinkCounter;
	
	// Lambda details
	private Regions region;
	private BasicAWSCredentials credentials;
	private static AWSLambda lambdaClient;
	private String functionName;
	
	
	@Override
	public void configure(Context context) {
		// set the AWS Region, defaulting to us-east-1
		String regionName = context.getString("region", "us-east-1");
		this.region = Regions.fromName(regionName);
		LOG.debug("[flume-aws-lambda] Region: " + region.getName());
		
		// set the lambda function name for this sink to invoke
		this.functionName = context.getString("functionName");
		LOG.debug("[flume-aws-lambda] Function: " + this.functionName);
		
		// get access and secret keys from context, configure AWS Credentials if
		// values are present...
		String accessKey = context.getString("accessKey");
		String secretKey = context.getString("secretKey");
		
		if (!StringUtils.isBlank(accessKey) && !StringUtils.isBlank(secretKey)) {
			LOG.debug("[flume-aws-lambda] Setting AWS credentials");
			this.credentials = new BasicAWSCredentials(accessKey, secretKey);
		}
		
		if (sinkCounter == null) {
	        sinkCounter = new SinkCounter(getName());
	    }
	}
	
	@Override
	public void start() {
		// initialize the lambda client
		AWSLambdaClientBuilder clientBuilder = null;
		
		if (this.credentials != null) {
			// {@see http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}
			clientBuilder = AWSLambdaClientBuilder.standard()
							.withCredentials(new AWSStaticCredentialsProvider(this.credentials))
							.withRegion(this.region);
		} else {
			clientBuilder = AWSLambdaClientBuilder.standard()
							.withRegion(this.region);
		}
		
		lambdaClient = clientBuilder.build();
		
		sinkCounter.start();
		super.start();
	}
	
	@Override
	public void stop() {
		sinkCounter.stop();
		super.stop();
	}
	
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		
		// start the transaction
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		
		try {
			Event event = channel.take();
			
			// if no events to process or an empty event, skip it
			// this generally happens when the channel is empty
			if (event == null || event.getBody().length == 0) {
				status = Status.BACKOFF;
				this.sinkCounter.incrementBatchEmptyCount();
			} else {
				String message = new String(event.getBody(), "UTF-8").trim();
				long timestamp = System.currentTimeMillis() / 1000L;
				LOG.debug("Received event with message: " + message);
				
				// {@see http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/basics-async.html}
				InvokeRequest request = new InvokeRequest()
						.withFunctionName(this.functionName)
						.withPayload("{\"message\":\"" + message + "\", \"timestamp\":" + timestamp + "}");
				
				// invoke the lambda function and inspect the result...
				// {@see http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/lambda/model/InvokeResult.html}
				InvokeResult result = lambdaClient.invoke(request);
				
				// Lambda will return an HTTP status code will be in the 200 range for successful
				// request, even if an error occurred in the Lambda function itself. Here, we check
				// if an error occurred via getFunctionError() before checking the status code.
				if ("Handled".equals(result.getFunctionError()) || "Unhandled".equals(result.getFunctionError())) {
					LOG.warn("Lambda function reported " + result.getFunctionError() + " function error");
					throw new EventDeliveryException("Failure invoking Lambda function: " + result.getFunctionError());
				} else if (result.getStatusCode() >= 200 && result.getStatusCode() < 300) {
					LOG.debug("Lambda function completed successfully");
					status = Status.READY;
					this.sinkCounter.incrementBatchCompleteCount();
				} else {
					LOG.debug("Lambda function error occurred");
					throw new EventDeliveryException("Failure invoking Lambda function with status code: " + result.getStatusCode());
				}
			}
			
			transaction.commit();
			this.sinkCounter.addToEventDrainSuccessCount(1);
		} catch (Throwable t) {
			transaction.rollback();
			// log the exception
			LOG.error("Transaction failed: " + t.getMessage());
			status = Status.BACKOFF;
			
			if (t instanceof Error) {
				throw (Error)t;
			} else {
				throw new EventDeliveryException(t);
			}
		} finally {
			transaction.close();
		}
		
		return status;
	}
	
	// cheating a bit here -- allowing lambda client to be set publicly so that we can inject
	// a mock version for testing ... else, need to look at PowerMock to inject private members
	public void setLambdaClient(AWSLambda client) {
		lambdaClient = client;
	}

}
