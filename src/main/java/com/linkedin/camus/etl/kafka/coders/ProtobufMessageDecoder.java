package com.linkedin.camus.etl.kafka.coders;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.Properties;

public class ProtobufMessageDecoder extends MessageDecoder<byte[], Message> {
    public static final String PROTOBUF_MESSAGE_CLASS_PREFIX = "camus.protobuf.message.class.";
    public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
    public static final String DEFAULT_TIMESTAMP_FIELD = "ts";

    private static org.apache.log4j.Logger log = Logger.getLogger(ProtobufMessageDecoder.class);
    private Method messageParseMethod;
    private String timestampFieldName;

    @Override
    public void init(Properties props, String topicName) {
        super.init(props, topicName);

        String messageClassForTopic = props.getProperty(PROTOBUF_MESSAGE_CLASS_PREFIX + topicName);
        if (messageClassForTopic == null || messageClassForTopic.isEmpty()) {
            throw new InvalidParameterException(PROTOBUF_MESSAGE_CLASS_PREFIX + topicName + " is required to be set");
        }

        timestampFieldName = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);

        try {
            Class<?> eventClass = Class.forName(messageClassForTopic);
            messageParseMethod = eventClass.getDeclaredMethod("parseFrom", byte[].class);
        } catch (ClassNotFoundException e) {
            throw new InvalidParameterException("Unable to load event class: " + e.getMessage());
        } catch (NoSuchMethodException e) {
            throw new InvalidParameterException("Unable to find parseFrom() method in protobuf message class: " + e.getMessage());
        }
    }

    @Override
    public CamusWrapper<Message> decode(byte[] payload) {
        if (payload.length < 1) {
            throw new RuntimeException("Empty payload!");
        }

        try {
            Message msg = (Message) messageParseMethod.invoke(null, payload);

            Descriptors.FieldDescriptor fd = msg.getDescriptorForType().findFieldByName(timestampFieldName);
            if (fd != null) {
                Object tsField = msg.getField(fd);
                if (tsField instanceof Timestamp) {
                    Timestamp ts = (Timestamp) tsField;
                    return new CamusWrapper<>(msg, Timestamps.toMillis(ts));
                }
            }

            log.warn("Couldn't find timestamp field '" + timestampFieldName + "' in Protobuf message, defaulting to current time.");

            return new CamusWrapper<>(msg, Instant.now().toEpochMilli());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Can't parse protobuf message, since parseMethod() is not callable: " + e.getMessage());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Can't parse protobuf message, since parseMethod() is not accessible: " + e.getMessage());
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Error parsing protobuf message: " + e.getCause().getMessage());
        }
    }
}
