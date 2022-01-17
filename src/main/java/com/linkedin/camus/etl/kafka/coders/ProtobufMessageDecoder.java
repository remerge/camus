package com.linkedin.camus.etl.kafka.coders;

import com.google.protobuf.Message;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.util.Properties;

public class ProtobufMessageDecoder extends MessageDecoder<byte[], Message> {
    public static final String PROTOBUF_MESSAGE_CLASS_PREFIX = "camus.protobuf.message.class.";

    private Method messageParseMethod;

    @Override
    public void init(Properties props, String topicName) {
        super.init(props, topicName);

        String messageClassForTopic = props.getProperty(PROTOBUF_MESSAGE_CLASS_PREFIX + topicName);
        if (messageClassForTopic == null || messageClassForTopic.isEmpty()) {
            throw new InvalidParameterException(PROTOBUF_MESSAGE_CLASS_PREFIX + topicName + " is required to be set");
        }

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
            return new CamusWrapper<>(msg, System.currentTimeMillis());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Can't parse protobuf message, since parseMethod() is not callable: " + e.getMessage());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Can't parse protobuf message, since parseMethod() is not accessible: " + e.getMessage());
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Error parsing protobuf message: " + e.getCause().getMessage());
        }
    }
}
