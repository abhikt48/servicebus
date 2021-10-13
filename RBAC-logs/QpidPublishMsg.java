/**
 * 
 */
package com.qpid.test;

import java.util.Enumeration;
import java.util.Hashtable;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.qpid.jms.JmsClientProperties;
import org.apache.qpid.jms.message.JmsMessage;
import org.mule.transport.servicebus.ServicebusConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used dependency -
 <dependency>
    <groupId>org.apache.qpid</groupId>
    <artifactId>qpid-jms-client</artifactId>
    <version>0.57.0</version>
</dependency>
*/

public class QpidPublishMsg{

    private static final String QUEUE_NAME = "ak";
    public static final String SBUS_NAME = "***";
    public static final String USERNAME = "***";
    public static final String PASSWORD = "***";
    private static final String QPID_CONNECTION_FACTORY_CLASS = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidPublishMsg.class);

    public void publishMessage() throws Exception {
        System.out.println("** Sent start **");

        Connection connection = createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        Destination destination = session.createQueue(QUEUE_NAME);

        MessageProducer messageProducer = session.createProducer(destination);

        Message message = session.createTextMessage("Hello World");
        message.setStringProperty(JmsClientProperties.JMSXGROUPID, "112233");
        
        ((JmsMessage) message).getFacade().setTracingAnnotation(ServicebusConstants.MSG_ANOT_PARTITION_KEY, "ABCD-112233");
        
        messageProducer.send(message);

        LOGGER.info("** publish finish **");
    }
    

    public static void main(String[] args) throws Exception {
        QpidPublishMsg testJMSXGROUPID = new QpidPublishMsg();
        testJMSXGROUPID.publishMessage();
    }

    private Connection createConnection() throws NamingException, JMSException {
        Hashtable<String, String> hashtable = new Hashtable<>();
        int maxFrameSize = 1000;
        hashtable.put("connectionfactory.SBCF", "amqps://" + SBUS_NAME + ".servicebus.windows.net?jms.validatePropertyNames=false");

        hashtable.put(Context.INITIAL_CONTEXT_FACTORY, QPID_CONNECTION_FACTORY_CLASS);

        Context context = new InitialContext(hashtable);
        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("SBCF");

        Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
        LOGGER.info("** Connection created, clientId '{}' **", connection.getClientID());
        return connection;
    }


}
