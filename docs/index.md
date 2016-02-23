# Stubbed Cassandra

Stubbed Cassandra is an open source tool that enables you to test applications that use Cassandra in a quick, deterministic way.

It is especially aimed edge case testing such as read and write timeouts.

It acts as a real Cassandra instance by implementing the server side of the native protocol and can be primed to respond with results or with exceptions like read timeouts. It does this by implementing the server side of the CQL binary protocol.

