# Reproducer for OrientDB issue 10453

See https://github.com/orientechnologies/orientdb/issues/10453.

## Preparation

1. Build docker images

```bash
$ docker build docker/orientdb-3_2_38/ -t orientdb-issue-10453:3.2.38 --build-context scripts=docker/scripts
$ docker build docker/orientdb-3_2_39/ -t orientdb-issue-10453:3.2.39 --build-context scripts=docker/scripts
$ docker build docker/orientdb-3_2_40/ -t orientdb-issue-10453:3.2.40 --build-context scripts=docker/scripts
```

## Testing Scenario

1. Start OrientDB 3.2.38

```bash
$ docker run -it --rm -p 2480:2480 -p 2424:2424 -p 1044:1044 \
    -v $PWD/docker/config:/orientdb/config \
    -v $PWD/docker/orientdb-server-metrics:/tmp/orientdb-server-metrics.csv \
    orientdb-issue-10453:3.2.38
```

2. Execute tests

```bash
$ ./mvnw clean test -D orientdb.version=<client version>
```

where `client version` is one of
- `3.2.38`
- `3.2.39`
- `3.2.40`

_Note: actually, any client version goes - it's a simple Maven dependency._

3. Observe content of `./docker/orientdb-server-metrics/server.network.sessions.csv`

```bash
$ less ./docker/orientdb-server-metrics/server.network.sessions.csv
```

4. Observed Results

- Maximum reported value of `server.network.sessions` - `29`.
- All tests pass successfully.

5. Stop OrientDB container

6. Start OrientDB 3.2.39 (or 3.2.40)

```bash
$ docker run -it --rm -p 2480:2480 -p 2424:2424 -p 1044:1044 \
    -v $PWD/docker/config:/orientdb/config \
    -v $PWD/docker/orientdb-server-metrics:/tmp/orientdb-server-metrics.csv \
    orientdb-issue-10453:3.2.39
``` 

7. Execute tests - see step 2 above.
8. Observe content of `./docker/orientdb-server-metrics/server.network.sessions.csv`

9. Observed Results

- Maximum reported value of `server.network.sessions` metric reaches values 30000 and more.
- Tests fail due to `OIOException Error on connecting to localhost:2424`. 
- OrientDB refuses connections and reports following message in the log

> WARNI Reached maximum number of concurrent connections (max=1000, current=4128), reject incoming connection from
> /172.25.81.115:45898 [OServerNetworkListener]

10. Stop OrientDB container
