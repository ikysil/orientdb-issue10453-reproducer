# Reproducer for OrientDB issue 10453

See https://github.com/orientechnologies/orientdb/issues/10453.

## Testing Scenario

1. Start OrientDB 3.2.39

```bash
docker build docker/orientdb-3_2_39/ -t orientdb-standalone:3.2.39
docker run -it --rm -p 2480:2480 -p 2424:2424 -p 1044:1044 \
  -v $PWD/docker/config:/orientdb/config orientdb-standalone:3.2.39
```

2. Open OrientDB Studio at `localhost:2480` and create a new database `Issue10453`
3. Start test `ReproduceIssue10453Test#defineSchemaBeforeWorkaround`
    - use debugger and put breakpoint at `logger.info("DONE")` statement
4. Start test `ReproduceIssue10453Test#defineSchemaAfterWorkaround`
    - keep `#defineSchemaBeforeWorkaround` at breakpoint
5. Observe message in the logs of OrientDB similar to

> WARNI Reached maximum number of concurrent connections (max=1000, current=4128), reject incoming connection from
> /172.25.81.115:45898 [OServerNetworkListener]

6. Compare behavior with OrientDB 3.2.38

```bash
docker build docker/orientdb-3_2_38/ -t orientdb-standalone:3.2.38
docker run -it --rm -p 2480:2480 -p 2424:2424 -p 1044:1044 \
  -v $PWD/docker/config:/orientdb/config orientdb-standalone:3.2.38
```

There are no _Reached maximum number of concurrent connections_ messages in the log when the same scenario is performed.
