package com.projectdarkstar.rpc.common;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer byteBuffer;

    public ByteBufferInputStream(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public int read() throws IOException {
        if (!byteBuffer.hasRemaining()) {
            return -1;
        }
        return byteBuffer.get();
    }

    public static Message buildMessage(Message prototype, ByteBuffer byteBuffer) {
        try {
            return prototype.toBuilder().mergeFrom(new ByteBufferInputStream(byteBuffer)).build();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            //Never happen
            throw new RuntimeException(e);
        }
    }

}
