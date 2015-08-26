## Current Client ##

**Since: 0.9.2**

`CurrentClient` offers insight into the current state of Scassandra in addition to being able to manipulate that state.

### Retrieving Active Connections ###

To observe the active connections using current client, simply make the following call:

```java
ConnectionReport report = currentClient.getConnections();
```

Additionally you can filter connections by host or retrieve an individual connection using the following methods:

```java
ConnectionReport getConnections(String ip);
ConnectionReport getConnections(InetAddress address);
ConnectionReport getConnection(String host, int port);
ConnectionReport getConnection(InetSocketAddress address);
```

**Note**: For consistency, the API will not throw exceptions if no matching connections are found.

### Closing Active Connections ###

To close all active connections using current client, make the following call:

```java
ClosedConnectionReport report = underTest.closeConnections(CLOSE);
```

Alternatively you can filter which connections to close by host or close an individual connection using the following methods:

```java
ClosedConnectionReport closeConnections(CloseType closeType, String ip);
ClosedConnectionReport closeConnections(CloseType closeType, InetAddress address);
ClosedConnectionReport closeConnection(CloseType closeType, String host, int port);
ClosedConnectionReport closeConnection(CloseType closeType, InetSocketAddress address);
```

The CloseType parameter determines how to close the connection and has the following definition.

* **`CLOSE`**: A normal close operation which will first flush pending writes and then close the socket.
* **`RESET`**: Will not flush pending writes and will result in a TCP RST packet being sent to the peer.
* **`HALFCLOSE`**: Will flush pending writes and then half-close the connection, waiting for the peer to close the other half.

The returned `ClosedConnectionReport` contains a list of connections that were closed and the `CloseType` that was used to close the connections.

**Note**: For consistency, the endpoint always returns a successful response, even if no connections were closed.

### Rejecting New Connections ###

To prevent Scassandra from accepting new connections, make the following call:

```java
currentClient.disableListener();
```

The API call will return a boolean indicating whether or not the state of the listener changed.  If the listener was already disabled it will return false.

Optionally, you may indicate that you want to start rejecting connections, but only after a certain number of accepted connections.  This may be done in the following manner:

```java
currentClient.disableListener(5);
```

This will accept the next 5 connections and then disable the listener.

### Accepting New Connections ###

If the listener was previously configured to reject connections, you may reenable the listener by performing the following operation:

```java
currentClient.enableListener();
```

Like [Rejecting New Connections](#rejecting-new-connections), the returned boolean will indicate whether or not the listening behavior has changed.
