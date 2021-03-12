/**
 * 
 */
package org.mule.transport.servicebus.transformers;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.qpid.jms.JmsConnection;
import org.junit.Test;
import org.mule.transport.servicebus.ServicebusCustomConnectionListener;

/**
 * @author Abhishek.Kumar
 *
 */
public class TestSessionClosedException {
    
    private static final String QUEUE_NAME = "test";
    public static final String SBUS_NAME = "**";
    public static final String USERNAME = "***";
    public static final String PASSWORD = "****";
    private static final String QPID_CONNECTION_FACTORY_CLASS = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";

    
    @Test(timeout = 20000)
    public void testRemotelyEndSessionWithMessageListener() throws Exception {
        final String BREAD_CRUMB = "ErrorDescriptionBreadCrumb";

        //try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            //JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            JmsConnection connection = (JmsConnection)createConnection();
            
            final CountDownLatch exceptionListenerFired = new CountDownLatch(1);
            final AtomicReference<JMSException> asyncError = new AtomicReference<JMSException>();
            connection.setExceptionListener(ex -> {
                asyncError.compareAndSet(null, ex);
                exceptionListenerFired.countDown();
            });

            final CountDownLatch sessionClosed = new CountDownLatch(1);
            connection.addConnectionListener(new ServicebusCustomConnectionListener() {
                @Override
                public void onSessionClosed(Session session, Throwable exception) {
                    sessionClosed.countDown();
                }
            });

            //testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QUEUE_NAME);

            // Create a consumer
            //testPeer.expectReceiverAttach();
            final MessageConsumer consumer = session.createConsumer(queue);

            // Expect credit to be sent when listener is added, then remotely close the session.
            //testPeer.expectLinkFlow();
            //testPeer.remotelyEndLastOpenedSession(true, 0, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);

            consumer.setMessageListener(m -> {
                // No-op
            });

            // Verify ExceptionListener fired
            assertTrue("ExceptionListener did not fire", exceptionListenerFired.await(5, TimeUnit.SECONDS));

            JMSException jmsException = asyncError.get();
            assertNotNull("Exception from listener was not set", jmsException);
            String message = jmsException.getMessage();
            //assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()) && message.contains(BREAD_CRUMB));

            // Verify the session (and  consumer) got marked closed and listener fired
           // testPeer.waitForAllHandlersToComplete(1000);
            assertTrue("Session closed callback did not fire", sessionClosed.await(5, TimeUnit.SECONDS));
            assertTrue("consumer never closed.", verifyConsumerClosure(BREAD_CRUMB, consumer));
            assertTrue("session never closed.", verifySessionClosure(BREAD_CRUMB, session));

            // Try closing consumer and session explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything unexpected.
            consumer.close();
            session.close();

            //testPeer.expectClose();
            connection.close();
        //}
    }
    
    private Connection createConnection() throws NamingException, JMSException {
        Hashtable<String, String> hashtable = new Hashtable<>();
        
        hashtable.put("connectionfactory.SBCF", "amqps://"+ SBUS_NAME +".servicebus.windows.net?jms.prefetchPolicy.all=0");
        
        hashtable.put(Context.INITIAL_CONTEXT_FACTORY, QPID_CONNECTION_FACTORY_CLASS);

        Context context = new InitialContext(hashtable);
        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("SBCF");
        
        Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
        return connection;
    }
    
    private boolean verifyConsumerClosure(final String BREAD_CRUMB, final MessageConsumer consumer) throws Exception {
        return Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                try {
                    consumer.getMessageListener();
                } catch (IllegalStateException jmsise) {
                    if (jmsise.getCause() != null) {
                        String message = jmsise.getCause().getMessage();
                        return message.contains("amqp:resource-limit-exceeded") &&
                                message.contains(BREAD_CRUMB);
                    } else {
                        return false;
                    }
                }
                return false;
            }
        }, 5000, 10);
    }

    private boolean verifySessionClosure(final String BREAD_CRUMB, final Session session) throws Exception {
        return Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                try {
                    session.getTransacted();
                } catch (IllegalStateException jmsise) {
                    if (jmsise.getCause() != null) {
                        String message = jmsise.getCause().getMessage();
                        return message.contains("amqp:resource-limit-exceeded") &&
                                message.contains(BREAD_CRUMB);
                    } else {
                        return false;
                    }
                }
                return false;
            }
        }, 5000, 10);
    }

}
