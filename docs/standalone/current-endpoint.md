## Current Endpoint ##

**Since: 0.9.2**

The `/current` endpoint offers insight into the current state of Scassandra in addition to being able to manipulate that state.

### Retrieving Active Connections ###

To observe the active connections to Scassandra, perform the following operation:

```
GET on http://[host]:[admin-port]/current/connections/[ip]/[port]
```

Where ip and port are optional.

#### Successful response ####

Here is an example of a response you may see when requesting all connections by performing a `GET` on `/current/connections`.  The response payload is an object with an array of host/port combinations for active connections.

```json
{
  "connections": [{
    "host": "127.0.0.1",
    "port": 60163
  }, {
    "host": "127.0.0.1",
    "port": 60164
  }, {
    "host": "127.0.0.1",
    "port": 60165
  }]
}
```

The following response shows the response payload of performing a `GET` on `/current/connections/127.0.0.1/60165`:

```json
{
  "connections": [{
    "host": "127.0.0.1",
    "port": 60165
  }]
}
```

**Note**: For consistency, the endpoint always returns successfully, even if no results are returned.

### Closing Active Connections ###

You can close all or select connections by performing the following operation:

```
DELETE on http://[host]:[admin-port]/current/connections/[ip]/[port][?type=close|reset|halfclose]
```

Where ip and port are optional.  Additionally you may provide a `type` parameter with either of the values `close`, `reset`, or `halfclose`.  This parameter determines how to close the connection and has the following definition.

* **`close`**: A normal close operation which will first flush pending writes and then close the socket (This is the default behavior).
* **`reset`**: Will not flush pending writes and will result in a TCP RST packet being sent to the peer.
* **`halfclose`**: Will flush pending writes and then half-close the connection, waiting for the peer to close the other half.

#### Successful Response ####

Here is an example of a response to closing all connections by performing a `DELETE` on `/current/connections`:

```json
{
  "closed_connections": [{
    "host": "127.0.0.1",
    "port": 63046
  }, {
    "host": "127.0.0.1",
    "port": 63047
  }, {
    "host": "127.0.0.1",
    "port": 63048
  }, {
    "host": "127.0.0.1",
    "port": 63049
  }, {
    "host": "127.0.0.1",
    "port": 63050
  }, {
    "host": "127.0.0.1",
    "port": 63051
  }, {
    "host": "127.0.0.1",
    "port": 63052
  }, {
    "host": "127.0.0.1",
    "port": 63053
  }, {
    "host": "127.0.0.1",
    "port": 63054
  }],
  "operation": "close"
}
```

The following response shows the response payload of performing a `DELETE` on `/current/connections/127.0.0.1/63158?type=halfclose`:

```json
{
  "closed_connections": [{
    "host": "127.0.0.1",
    "port": 63158
  }],
  "operation": "halfclose"
}
```

**Note**: For consistency, the endpoint always returns a successful response, even if no connections were closed.

### Rejecting New Connections ###

To prevent Scassandra from accepting new connections, perform the following operation:

```
DELETE on http://[host]:[admin-port]/server/listener[?after=0]
```

The response payload will indicate whether or not the listening behavior has changed.  If new connections were not already being rejected, it will return an attribute "changed" with the value true, otherwise false.  For example:

```json
{
  "changed": true
}
```

Optionally, you may indicate that you want to start rejecting connections, but only after a certain number of accepted connections.  To do this, pass in the query parameter 'after' with an integer value as part of the URL.  For example if you pass in a value of 5, the next 5 connections will be accepted and the rest rejected.

### Accepting New Connections ###

If the listener was previously configured to reject connections, you may reenable the listener by performing the following operation:

```
PUT on http://[host]:[admin-port]/server/listener
```

Like [Rejecting Connections](#rejecting-connections), the response payload will indicate whether or not the listening behavior has changed.
