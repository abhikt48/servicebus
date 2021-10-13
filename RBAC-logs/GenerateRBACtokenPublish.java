/**
 * 
 */
package com.sbus.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import reactor.core.publisher.Hooks;

/**
 * Used dependency
 <dependency>
    <groupId>com.azure</groupId>
    <artifactId>azure-messaging-servicebus</artifactId>
    <version>7.3.0</version>
</dependency>
<dependency>
    <groupId>com.azure</groupId>
    <artifactId>azure-identity</artifactId>
    <version>1.3.6</version>
</dependency>*/
public class GenerateRBACtokenPublish {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateRBACtokenPublish.class);
    private static String fullyQualifiedNamespace = "***.servicebus.windows.net";
    private static String queueName = "ak";

    public static void main(String[] args) {

        try {
            Hooks.onOperatorDebug();

            LOGGER.info("** Publish start **");
            ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                                     .clientId("***")
                                     .clientSecret("**")
                                     .tenantId("***").build();

            ServiceBusSenderClient serviceBusSenderClient = new ServiceBusClientBuilder().credential(fullyQualifiedNamespace, clientSecretCredential).sender().queueName(queueName).buildClient();

            ServiceBusMessage serviceBusMessage = new ServiceBusMessage("Hello World");
            serviceBusSenderClient.sendMessage(serviceBusMessage);

            LOGGER.info("** Publish finished **");

        } catch (Exception e) {
            LOGGER.error("Failed to publish", e);
            e.printStackTrace();
        }


    }
}
