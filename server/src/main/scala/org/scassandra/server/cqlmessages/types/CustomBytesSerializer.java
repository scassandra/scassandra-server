package org.scassandra.server.cqlmessages.types;

import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import java.nio.ByteBuffer;


public class CustomBytesSerializer implements TypeSerializer<byte[]> {

    public static final CustomBytesSerializer instance = new CustomBytesSerializer();

    public ByteBuffer serialize(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes);
    }

    public byte[] deserialize(ByteBuffer value)
    {
        return ByteBufferUtil.getArray(value);
    }

    public void validate(ByteBuffer value) throws MarshalException
    {
        // all bytes are legal.
    }

    public String toString(byte[] value) {
        return Hex.bytesToHex(value);
    }

    public Class<byte[]> getType()
    {
        return byte[].class;
    }
}
