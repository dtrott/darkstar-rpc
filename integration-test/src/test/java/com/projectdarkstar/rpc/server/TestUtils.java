package com.projectdarkstar.rpc.server;

import com.example.Example.StringPair;
import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.ByteBuffer;

public class TestUtils {

    public static StringPair buildPair(final String name, final String value) {
        return StringPair.newBuilder().setName(name).setValue(value).build();
    }

    public static StringPair buildPair(ByteBuffer buffer) throws InvalidProtocolBufferException {
        return StringPair.newBuilder().mergeFrom(buffer.array(), buffer.position(), buffer.remaining()).build();
    }
}
