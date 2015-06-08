package common;

import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public interface CassandraRow {

    Date getDate(String columnName);

    String getString(String name);

    int getInt(String age);

    BigDecimal getDecimal(String adecimal);

    ByteBuffer getBytes(String blob);

    boolean getBool(String aBoolean);

    double getFloat(String name);

    long getLong(String name);

    <T> Set<T> getSet(String name, Class<T> setType);

    <T> List<T> getList(String name, Class<T> listType);

    <K, V> Map<K, V>getMap(String name, Class<K> keyType, Class<V> valueType);

    InetAddress getInet(String name);

    BigInteger getVarint(String name);

    double getDouble(String name);

    UUID getUUID(String name);
}
