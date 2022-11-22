package com.datastax.astra.astrastreaming.examples;

import org.apache.pulsar.client.api.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.client.api.schema.GenericRecord;

public class SimpleConsumer {

    private static final String SERVICE_URL = "pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651";

    public static void main(String[] args) throws IOException
    {

        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(
                        AuthenticationFactory.token("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NjkwMTcwMjUsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50O2UwZTdhNmEzLTRjNWQtNDM2My1hNzYwLTJlMmE0ODA1MjkwODtjM1J5WldGdGRHVnVZVzUwOzE1YzA5MzllMjMiLCJ0b2tlbmlkIjoiMTVjMDkzOWUyMyJ9.kAwbfOypZg6GEN_eCpoymE-LfXCx1sXBBYcq9JvXxn-qNB-MKz3sb2-r7yFZfwYmtNMth3n3QPIt5cGSW_roB0bgta4OqH3R86MpzvmTcKT--vHGFetPKe7B26uPcOPIj7m7fpX6a4tuGcRyRKYwYA3EVYKk7bp3xa5u9Ia--NbCEdIpKTw0JEWRVGCkGukyfkAansptcsS586UhBuFlbLGlLkacTf2JWNA5bshv7DncsqX6MmPzKmg2vO6vR-RfWFtYsdyyZuqd5FiWeTJEgdbEo75OYnBPdA5nO8WkYCjaLuA64f9IVyOH1EKw6DQNmtzsd4XZFKWTqLK8HLzKZw")
                )
                .build();

        // Create consumer on a topic with a subscription
        Consumer consumer = client.newConsumer()
                .topic("streamtenant/astracdc/data-b128616b-da57-4bc7-b614-9fba532fbdcc-useast1ks.t1")
                .subscriptionName("my-java-sub")
                .subscribe();

        boolean receivedMsg = false;
        // Loop until a message is received
        do {
            // Block for up to 1 second for a message
            Message msg = consumer.receive(1, TimeUnit.SECONDS);

            if(msg != null){
                System.out.printf("Message received: %s", new String(msg.getData()));

                // Acknowledge the message to remove it from the message backlog
                consumer.acknowledge(msg);

                receivedMsg = true;
            }

        } while (!receivedMsg);

        //Close the consumer
       // consumer.close();

        // Close the client
        //client.close();

    }

}