package com.datastax.astra.astrastreaming.examples;

import org.apache.pulsar.client.api.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;

public class CDCSchemaAwareConsumer {

    private static final String SERVICE_URL = "pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651";

    public static void main(String[] args) throws IOException
    {

        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(
                        AuthenticationFactory.token("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NjkwOTE5NjksImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50O2UwZTdhNmEzLTRjNWQtNDM2My1hNzYwLTJlMmE0ODA1MjkwODtjM1J5WldGdGRHVnVZVzUwOzk0OWZhODYyNjIiLCJ0b2tlbmlkIjoiOTQ5ZmE4NjI2MiJ9.C5uoZlDa64ZINKEHfTTp9BNtiK0zFDoA4J0gM53BM13OaYUvMnrjsmCA0QWLZoduOFfgd5-I_RWWLIyl3q1u_b52ZT2npxMsyzwBBy_0D7MUg1bloUJ1Lvl8iD-osdlZUM1wuJq6-VOEssQZD-rI_K3lDbQtmacqJg-PIlrGZ0X5NQLtJeg4aiyoBokMJ_uWa3zXDvqoEhI_SkAfl3nHfpj9sIdNeq4efYsuMxQd3bvpT7OS-7KL7ZJWmZHaq6G7eqjXQve0Uc-_Y-tq_khTfHaMKWIkpmXBTgMdrJoJ_kv4sjoInETkIFDhCRbsaOY1qd84MYGxnjM2VWCtAeOOXg")
                )
                .build();

        // Create consumer on a topic with a subscription
        Consumer<GenericRecord> consumer = client.newConsumer(Schema.AUTO_CONSUME())
                .topic("streamtenant/astracdc/data-08ed3753-783d-49e7-9726-e95808032a9e-myks.t1")
                .subscriptionName("testsub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        boolean receivedMsg = false;
        // Loop until a message is received
        do {
            // Block for up to 1 second for a message
            Message<GenericRecord> msg  = consumer.receive(1, TimeUnit.SECONDS);

            if(msg != null){
                GenericRecord input = msg.getValue();
                //CDC Uses KeyValue Schema
                KeyValue<GenericRecord, GenericRecord> keyValue = (KeyValue<GenericRecord, GenericRecord>) input.getNativeObject();

                GenericRecord keyGenRecord = keyValue.getKey();
                displayGenericRecordFields("key",keyGenRecord);

                GenericRecord valGenRecord = keyValue.getValue();
                displayGenericRecordFields("value",valGenRecord);

                // Acknowledge the message to remove it from the message backlog
                consumer.acknowledge(msg);

                receivedMsg = true;
            }

        } while (!receivedMsg);

        //Close the consumer
        consumer.close();

        // Close the client
        client.close();

    }

    private static void displayGenericRecordFields(String recordName,GenericRecord genericRecord) {
        System.out.printf("---Fields in %s: ", recordName);
        genericRecord.getFields().stream().forEach((fieldtmp) ->
                System.out.printf(" %s = %s",fieldtmp.getName(),genericRecord.getField(fieldtmp) ));
        System.out.println(" ---");
    }

}