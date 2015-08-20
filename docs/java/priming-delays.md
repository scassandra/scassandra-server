## Priming Delays

**Since: 0.4.0**

Stubbed Cassandra allows you to slow down responses to simulate slow queries.

This is useful if you have client side timeouts implemented and you want to test them.

To do this from the Java API for either Queries or Prepared statements just add the withFixedDelay (in milliseconds).

```java
PrimingRequest primingRequest = PrimingRequest.queryBuilder()
        .withFixedDelay(primedDelay)
        .withQuery(query)
        .build();
primingClient.prime(primingRequest);
```