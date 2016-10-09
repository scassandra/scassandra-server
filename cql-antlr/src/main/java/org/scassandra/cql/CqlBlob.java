package org.scassandra.cql;

import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;

public class CqlBlob extends PrimitiveType {
    CqlBlob() {
        super("blob");
    }
    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) {
            throw throwNullError(actual, this);
        }
        if (expected instanceof String) {
            return expected.equals(actual);
        } else if (expected instanceof ByteBuffer) {
            ByteBuffer bb = (ByteBuffer) expected;
            byte[] b = new byte[bb.remaining()];
            bb.get(b);
            String encodedExpected = Hex.encodeHexString(b);
            String actualWithout0x = actual.toString().replaceFirst("0x", "");
            return encodedExpected.equals(actualWithout0x);
        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }
}
