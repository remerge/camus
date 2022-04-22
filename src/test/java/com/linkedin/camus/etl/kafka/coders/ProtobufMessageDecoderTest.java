package com.linkedin.camus.etl.kafka.coders;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.linkedin.camus.coders.CamusWrapper;
import events.protos.TestMessage;
import mockit.Mock;
import mockit.MockUp;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.security.InvalidParameterException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ProtobufMessageDecoderTest {
    static final String topicName = "test_topic";
    static final String currentTime = "2022-01-01T12:00:00Z";
    static final Clock currentTimeClock = Clock.fixed(Instant.parse(currentTime), ZoneId.systemDefault());

    private Properties properties;
    private ProtobufMessageDecoder decoder;

    @Before
    public void beforeTest() {
        properties = new Properties();
        decoder = new ProtobufMessageDecoder();
    }

    @Test
    public void testInitMessageClassNameDoesntExist() {
        InvalidParameterException exception = assertThrows(
                InvalidParameterException.class,
                () -> decoder.init(properties, topicName));

        assertEquals(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName + " is required to be set", exception.getMessage());
    }

    @Test
    public void testInitMessageClassNameIsEmpty() {
        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX + topicName, "");

        InvalidParameterException exception = assertThrows(
                InvalidParameterException.class,
                () -> decoder.init(properties, topicName));

        assertEquals(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName + " is required to be set", exception.getMessage());
    }

    @Test
    public void testInitMessageClassDoesntExist() {
        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, "WrongClass");

        InvalidParameterException exception = assertThrows(
                InvalidParameterException.class,
                () -> decoder.init(properties, topicName));

        assertThat(exception.getMessage(), CoreMatchers.containsString("Unable to load event class"));
    }

    @Test
    public void testInitMessageClassDoesntHaveParseMethod() {
        class WrongClass {
        }

        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, WrongClass.class.getName());

        InvalidParameterException exception = assertThrows(
                InvalidParameterException.class,
                () -> decoder.init(properties, topicName));

        assertThat(exception.getMessage(),
                CoreMatchers.containsString("Unable to find parseFrom() method in protobuf message class"));
    }

    @Test
    public void testInitMessageClassSucceeded() {
        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, TestMessage.BasicTypes.class.getName());
        decoder.init(properties, topicName);
    }

    @Test
    public void testDecodeEmptyPayload() {
        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, TestMessage.BasicTypes.class.getName());
        decoder.init(properties, topicName);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> decoder.decode(new byte[]{}));

        assertThat(exception.getMessage(), CoreMatchers.containsString("Empty payload"));
    }

    @Test
    public void testDecodeWrongProtobufMessage() {
        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, TestMessage.BasicTypes.class.getName());
        decoder.init(properties, topicName);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> decoder.decode(new byte[]{1, 2, 3}));

        assertThat(exception.getMessage(), CoreMatchers.containsString("Error parsing protobuf message"));
    }

    @Test
    public void testDecodeSucceeded() {
        new MockUp<Instant>() {
            @Mock
            public Instant now() {
                return Instant.now(currentTimeClock);
            }
        };

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
        assertEquals(expectedMsg, resultMsg);
        assertEquals(currentTimeClock.millis(), result.getTimestamp());
    }

    @Test
    public void testDecodeWithTimestampSucceeded() {
        properties.setProperty(ProtobufMessageDecoder.PROTOBUF_MESSAGE_CLASS_PREFIX +
                topicName, TestMessage.BasicTypesWithTimestamp.class.getName());
        decoder.init(properties, topicName);

        Timestamp ts = Timestamp.newBuilder()
                .setSeconds(TimeUnit.MILLISECONDS.toSeconds(currentTimeClock.millis()))
                .build();

        TestMessage.BasicTypesWithTimestamp expectedMsg = TestMessage.BasicTypesWithTimestamp.newBuilder()
                .setTs(ts)
                .setIntValue(1)
                .setStringValue("test")
                .setFloatValue(1.0f)
                .build();

        CamusWrapper<? extends Message> result = decoder.decode(expectedMsg.toByteArray());

        Message resultMsg = result.getRecord();
        assertEquals(expectedMsg, resultMsg);
        assertEquals(Timestamps.toMillis(ts), result.getTimestamp());
    }
}
