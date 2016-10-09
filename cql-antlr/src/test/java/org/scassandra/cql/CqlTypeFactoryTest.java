package org.scassandra.cql;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CqlTypeFactoryTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"map<ascii,ascii>", new MapType(PrimitiveType.ASCII, PrimitiveType.ASCII) },
                {"map<blob,inet>", new MapType(PrimitiveType.BLOB, PrimitiveType.INET) },
                {"map<uuid,timeuuid>", new MapType(PrimitiveType.UUID, PrimitiveType.TIMEUUID) },
                {"set<ascii>", new SetType(PrimitiveType.ASCII) },
                {"set<inet>", new SetType(PrimitiveType.INET) },
                {"list<ascii>", new ListType(PrimitiveType.ASCII) },
                {"list<boolean>", new ListType(PrimitiveType.BOOLEAN) },
                {"list<decimal>", new ListType(PrimitiveType.DECIMAL) },
                {"ascii", PrimitiveType.ASCII},
                {"varchar", PrimitiveType.VARCHAR},
                {"bigint", PrimitiveType.BIG_INT},
                {"blob", PrimitiveType.BLOB},
                {"boolean", PrimitiveType.BOOLEAN},
                {"counter", PrimitiveType.COUNTER},
                {"decimal", PrimitiveType.DECIMAL},
                {"double", PrimitiveType.DOUBLE},
                {"float", PrimitiveType.FLOAT},
                {"int", PrimitiveType.INT},
                {"timestamp", PrimitiveType.TIMESTAMP},
                {"varint", PrimitiveType.VAR_INT},
                {"timeuuid", PrimitiveType.TIMEUUID},
                {"inet", PrimitiveType.INET},
                {"text", PrimitiveType.TEXT}
        });
    }

    private String input;

    private CqlType expectedType;

    public CqlTypeFactoryTest(String input, CqlType expectedType) {
        this.input = input;
        this.expectedType = expectedType;
    }

    private CqlTypeFactory underTest = new CqlTypeFactory();

    @Test
    public void test() throws Exception {
        assertEquals(expectedType, underTest.buildType(input));
    }
}