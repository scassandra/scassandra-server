### Conflicting HTTP client versions

#### Description

The priming client and activity client use 4.3+ of HTTP client. If your production code uses an older version and you use the eclipse or intellij plugin to generate project artifacts this can often lead to the older production version of http client being used for the test and you get either a NoSuchField or NoSuchMethod error.

The Datastsax Java driver has a transitive dependency on HTTP client so this is likely to happen if you use version 1.* op the Java Driver as it depends on an old version.

So far I've always upgraded the version of HTTP client to 4.3+ and it hasn't affected the Datastax Java driver. However I realize upgrading production dependencies is not always possible and could release .

#### Example error:

```
java.lang.NoSuchFieldError: INSTANCE
	at org.apache.http.impl.io.DefaultHttpRequestWriterFactory.<init>(DefaultHttpRequestWriterFactory.java:52)
	at org.apache.http.impl.io.DefaultHttpRequestWriterFactory.<init>(DefaultHttpRequestWriterFactory.java:56)
	at org.apache.http.impl.io.DefaultHttpRequestWriterFactory.<clinit>(DefaultHttpRequestWriterFactory.java:46)
	at org.apache.http.impl.conn.ManagedHttpClientConnectionFactory.<init>(ManagedHttpClientConnectionFactory.java:72)
	at org.apache.http.impl.conn.ManagedHttpClientConnectionFactory.<init>(ManagedHttpClientConnectionFactory.java:84)
	at org.apache.http.impl.conn.ManagedHttpClientConnectionFactory.<clinit>(ManagedHttpClientConnectionFactory.java:59)
	at org.apache.http.impl.conn.PoolingHttpClientConnectionManager$InternalConnectionFactory.<init>(PoolingHttpClientConnectionManager.java:487)
	at org.apache.http.impl.conn.PoolingHttpClientConnectionManager.<init>(PoolingHttpClientConnectionManager.java:147)
	at org.apache.http.impl.conn.PoolingHttpClientConnectionManager.<init>(PoolingHttpClientConnectionManager.java:136)
	at org.apache.http.impl.conn.PoolingHttpClientConnectionManager.<init>(PoolingHttpClientConnectionManager.java:112)
	at org.apache.http.impl.client.HttpClientBuilder.build(HttpClientBuilder.java:726)
	at org.apache.http.impl.client.HttpClients.createDefault(HttpClients.java:58)
	at org.scassandra.http.client.PrimingClient.<init>(PrimingClient.java:26)
```

#### Solution

Use 4.3.3 of http client.
E.g add this to your gradle:

```
compile 'org.apache.httpcomponents:httpclient:4+'
```