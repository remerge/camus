package com.linkedin.camus.etl.kafka.coders;

import com.google.protobuf.Message;
import com.linkedin.camus.coders.CamusWrapper;
import events.protos.TestMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.security.InvalidParameterException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ProtobufMessageDecoderTest {
    static final String topicName = "test_topic";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Properties properties;
    private ProtobufMessageDecoder decoder;

    @Before
    public void initTest() {
        properties = new Properties();
        decoder = new ProtobufMessageDecoder();
    }

    @Test
    public void testInitMessageClassNameDoesntExist() {
        thrown.expect(InvalidParameterException.class);
        thrown.expectMessage(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName + " is required to be set");

        decoder.init(properties, topicName);
    }

    @Test
    public void testInitMessageClassNameIsEmpty() {
        thrown.expect(InvalidParameterException.class);
        thrown.expectMessage(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName + " is required to be set");

        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX + topicName, "");
        decoder.init(properties, topicName);
    }

    @Test
    public void testInitMessageClassDoesntExist() {
        thrown.expect(InvalidParameterException.class);
        thrown.expectMessage("Unable to load event class");

        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, "WrongClass");
        decoder.init(properties, topicName);
    }

    @Test
    public void testInitMessageClassDoesntHaveParseMethod() {
        class WrongClass {
        }

        thrown.expect(InvalidParameterException.class);
        thrown.expectMessage("Unable to find parseFrom() method in protobuf message class");

        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, WrongClass.class.getName());
        decoder.init(properties, topicName);
    }

    @Test
    public void testInitMessageClassSucceeded() {
        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, TestMessage.BasicTypes.class.getName());
        decoder.init(properties, topicName);
    }

    @Test
    public void testDecodeEmptyPayload() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Empty payload");

        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, TestMessage.BasicTypes.class.getName());
        decoder.init(properties, topicName);

        decoder.decode(new byte[]{});
    }

    @Test
    public void testDecodeWrongProtobufMessage() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Error parsing protobuf message");

        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, TestMessage.BasicTypes.class.getName());
        decoder.init(properties, topicName);

        decoder.decode(new byte[]{1, 2, 3});
    }

    @Test
    public void testDecodeSucceeded() {
        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, TestMessage.BasicTypes.class.getName());
        decoder.init(properties, topicName);

        TestMessage.BasicTypes expectedMsg = TestMessage.BasicTypes.newBuilder()
                .setIntValue(1)
                .setStringValue("test")
                .setFloatValue(1.0f)
                .build();

        CamusWrapper<? extends Message> result = decoder.decode(expectedMsg.toByteArray());

        Message resultMsg = result.getRecord();
        assertEquals(resultMsg, expectedMsg);
    }
}
