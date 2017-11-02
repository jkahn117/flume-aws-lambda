package com.jkahn.flume.sink.aws.lambda;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.Charset;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

import org.mockito.*;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.TooManyRequestsException;

import junit.framework.TestCase;

public class LambdaSinkTest extends TestCase {
	
	// 
	private String message = "hello world";
	private Event mockEvent = EventBuilder.withBody(message, Charset.forName("UTF-8"));
	
	/**
	 * Tests the {@link LambdaSink#process()} method, specifically success path where
	 * Lambda is properly invoked.
	 * 
	 * <p>
	 * <pre>
	 * Expected Results:
	 *  status = READY
     *  transaction.begin() is invoked
     *  transaction.commit() is invoked
     *  transaction.close() is invoked
	 * </pre>
	 * </p>
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProcessCompleted() throws Exception {
		// 
		LambdaSink uSink = new LambdaSink();
		LambdaSink mockSink = Mockito.spy(uSink);
		
		// mock context
		Context mockContext = Mockito.mock(Context.class);
		Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn("us-east-1");
		Mockito.when(mockContext.getString("functionName")).thenReturn("fakeFunction");
		Mockito.when(mockContext.getString("accessKey")).thenReturn("FAKE_ACCESS_KEY");
		Mockito.when(mockContext.getString("secretKey")).thenReturn("FAKE_SECRET_KEY");
		mockSink.configure(mockContext);
		
		// mock channel and transaction
		Channel mockChannel = Mockito.mock(Channel.class);
		mockSink.setChannel(mockChannel);  // AbstractSink uses private variable, not getChannel internally
		Transaction mockTransaction = Mockito.mock(Transaction.class);
		Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
		Mockito.when(mockChannel.take()).thenReturn(mockEvent);
		Mockito.when(mockSink.getChannel()).thenReturn(mockChannel);
		mockSink.start();  // does this need to be called like this? else not triggered in test case
		
		// mock lambda invoke -- must set lambda client after start(), else it will override
		AWSLambda mockLambdaClient = Mockito.mock(AWSLambda.class);
		InvokeResult mockResult = Mockito.mock(InvokeResult.class);
		Mockito.when(mockResult.getStatusCode()).thenReturn(new Integer(200));
		Mockito.when(mockLambdaClient.invoke(Mockito.any(InvokeRequest.class))).thenReturn(mockResult);
		mockSink.setLambdaClient(mockLambdaClient);
		
		// test result
		Sink.Status status = mockSink.process();
		assertEquals(Sink.Status.READY, status);
		Mockito.verify(mockTransaction, Mockito.times(1)).begin();
		Mockito.verify(mockTransaction, Mockito.times(1)).commit();
		Mockito.verify(mockTransaction, Mockito.times(1)).close();
	}
	
	
	/**
	 * Tests the {@link LambdaSink#process()} method, specifically path in which
	 * Lambda returns an error code due to failure.
	 * 
	 * <p>
	 * <pre>
	 * Expected Results:
	 *  status = READY
     *  transaction.begin() is invoked
     *  transaction.commit() is invoked
     *  transaction.close() is invoked
	 * </pre>
	 * </p>
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProcessLambdaError() throws Exception {
		// 
		LambdaSink uSink = new LambdaSink();
		LambdaSink mockSink = Mockito.spy(uSink);
		
		// mock context
		Context mockContext = Mockito.mock(Context.class);
		Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn("us-east-1");
		Mockito.when(mockContext.getString("functionName")).thenReturn("fakeFunction");
		Mockito.when(mockContext.getString("accessKey")).thenReturn("FAKE_ACCESS_KEY");
		Mockito.when(mockContext.getString("secretKey")).thenReturn("FAKE_SECRET_KEY");
		mockSink.configure(mockContext);
		
		// mock channel and transaction
		Channel mockChannel = Mockito.mock(Channel.class);
		mockSink.setChannel(mockChannel);  // AbstractSink uses private variable, not getChannel internally
		Transaction mockTransaction = Mockito.mock(Transaction.class);
		Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
		Mockito.when(mockChannel.take()).thenReturn(mockEvent);
		Mockito.when(mockSink.getChannel()).thenReturn(mockChannel);
		mockSink.start();  // does this need to be called like this? else not triggered in test case
		
		// mock lambda invoke -- must set lambda client after start(), else it will override
		AWSLambda mockLambdaClient = Mockito.mock(AWSLambda.class);
		InvokeResult mockResult = Mockito.mock(InvokeResult.class);
		Mockito.when(mockResult.getStatusCode()).thenReturn(new Integer(400)); // some error response
		Mockito.when(mockLambdaClient.invoke(Mockito.any(InvokeRequest.class))).thenReturn(mockResult);
		mockSink.setLambdaClient(mockLambdaClient);
		
		// test result
		Sink.Status status = mockSink.process();
		assertEquals(Sink.Status.BACKOFF, status);
		Mockito.verify(mockTransaction, Mockito.times(1)).begin();
		Mockito.verify(mockTransaction, Mockito.times(1)).rollback();
		Mockito.verify(mockTransaction, Mockito.times(1)).close();
	}
	
	/**
	 * Tests the {@link LambdaSink#process()} method, specifically path in which
	 * an exception is raised when invoking Lambda.
	 * 
	 * <p>
	 * <pre>
	 * Expected Results:
	 *  status = READY
     *  transaction.begin() is invoked
     *  transaction.close() is invoked
     *  EventDeliveryException with cause TooManyRequestsException is thrown
	 * </pre>
	 * </p>
	 * 
	 * @throws Exception
	 */
	//@SuppressWarnings("unchecked")
	@Test
	public void testProcessException() throws EventDeliveryException {
		// 
		LambdaSink uSink = new LambdaSink();
		LambdaSink mockSink = Mockito.spy(uSink);
		
		// mock context
		Context mockContext = Mockito.mock(Context.class);
		Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn("us-east-1");
		Mockito.when(mockContext.getString("functionName")).thenReturn("fakeFunction");
		Mockito.when(mockContext.getString("accessKey")).thenReturn("FAKE_ACCESS_KEY");
		Mockito.when(mockContext.getString("secretKey")).thenReturn("FAKE_SECRET_KEY");
		mockSink.configure(mockContext);
		
		// mock channel and transaction
		Channel mockChannel = Mockito.mock(Channel.class);
		mockSink.setChannel(mockChannel);  // AbstractSink uses private variable, not getChannel internally
		Transaction mockTransaction = Mockito.mock(Transaction.class);
		Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
		Mockito.when(mockChannel.take()).thenReturn(mockEvent);
		Mockito.when(mockSink.getChannel()).thenReturn(mockChannel);
		mockSink.start();  // does this need to be called like this? else not triggered in test case
		
		// mock lambda invoke -- must set lambda client after start(), else it will override
		AWSLambda mockLambdaClient = Mockito.mock(AWSLambda.class);
		Mockito.when(mockLambdaClient.invoke(Mockito.any(InvokeRequest.class))).thenThrow(new TooManyRequestsException(""));
		mockSink.setLambdaClient(mockLambdaClient);
		
		// test result
		assertThrows(EventDeliveryException.class, () -> {
			mockSink.process();
		});
	}
	
}
