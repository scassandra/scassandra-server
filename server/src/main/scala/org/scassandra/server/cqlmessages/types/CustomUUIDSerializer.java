package org.scassandra.server.cqlmessages.types;

import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import java.nio.ByteBuffer;
import java.util.UUID;

public class CustomUUIDSerializer implements TypeSerializer<UUID>
{
    public static final CustomUUIDSerializer instance = new CustomUUIDSerializer();

    public UUID deserialize(ByteBuffer bytes)
    {
        return bytes.remaining() == 0 ? null : CustomUUIDGen.getUUID(bytes);
    }

    public ByteBuffer serialize(UUID value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBuffer.wrap(CustomUUIDGen.decompose(value));
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 16 && bytes.remaining() != 0)
            throw new MarshalException(String.format("UUID should be 16 or 0 bytes (%d)", bytes.remaining()));
        // not sure what the version should be for this.
    }

    public String toString(UUID value)
    {
        return value == null ? "" : value.toString();
    }

    public Class<UUID> getType()
    {
        return UUID.class;
    }
}

