# Scylla

## Run tests on ScyllaDB 4.6

let's say we have a working directory `test`.
- `cd test/`

1. clone [jepsen repository](https://github.com/jepsen-io/jepsen.git) into directory `test/jepsen`
  - `git clone https://github.com/jepsen-io/jepsen`
2. clone this repository into `test/jepsen/scylla-jepsen-test`
  - `cd jepsen/`
  - `git clone git@github.com:Rivers-Shall/scylla-jepsen-test.git`
3. start up jepsen docker container with `test/jepsen/docker/bin/up`
  - `cd docker/bin/`
  - `sudo ./up` # this step takes time ...
    - You need to install `docker-compose` first.
      - Install `docker-desktop`
      - Download [`docker-compose v2.10.0`](https://github.com/docker/compose/releases/tag/v2.10.0) to `/usr/local/bin/docker-compose` and make it executable `sudo chmod +x /usr/local/bin/docker-compose`
4. enter jepsen-control node with `test/jepsen/docker/bin/console`
5. install ScyllaDB 4.6 on 5 nodes with `ssh n1/n2/n3/n4/n5` and `curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-version 4.6` 
6. enter `/jepsen/scylla-jepsen-test` directory in jepsen-control node
7. run test with `lein run test -w cas-only-register --version 4.6` and other options like `concurrency, rate, nemesis`

This repository implements Jepsen tests for Scylla.

You'll need a Jepsen environment, including a control node with a JVM and
[Leiningen](https://leiningen.org/), and a collection of Debian 10 nodes to
install the database on. There are instructions for setting up a Jepsen
environent [here](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment).

Once your environment is ready, you should be able to run something like:

```
lein run test -w list-append --concurrency 10n -r 100 --max-writes-per-key 100 --nemesis partition
```

This runs a single test using the `list-append` workload, which uses SERIAL reads, and LWT `UPDATE`s to append unique integers to CQL lists, then searches for anomalies which would indicate the resulting history is incompatible with strict serializability. The test uses ten times the number of DB nodes, runs approximately 100 requests per second, and writes up to 100 values per key. During the test, we introduce network partitions. For version 4.2, this might yield something like:

```clj
  :anomaly-types (:G-nonadjacent-realtime
                  :G-single-realtime
                  :cycle-search-timeout
                  :incompatible-order),
  ...
  :not #{:read-atomic :read-committed},
  :also-not
  #{:ROLA :causal-cerone :consistent-view :cursor-stability
    :forward-consistent-view :monotonic-atomic-view
    :monotonic-snapshot-read :monotonic-view
    :parallel-snapshot-isolation :prefix :repeatable-read :serializable
    :snapshot-isolation :strict-serializable
    :strong-session-serializable :strong-session-snapshot-isolation
    :strong-snapshot-isolation :update-serializable}},
 :valid? false}

Analysis invalid! (?????????????????? ?????????
```

## Running Tests

Use `lein run test -w ...` to run a single test. Use `lein run test-all ...` to
run a broad collection of workloads with a variety of nemeses. You can filter
`test-all` to just a specific workload or nemesis by using `-w` or `--nemesis`.

As with all Jepsen tests, you'll find detailed results, graphs, and node logs
in `store/latest`. `lein run serve` will start a web server on port 8080 for
browsing the `store` directory.

Most tests are tunable with command line options. See `lein run test --help`
for a full list of options.

## Running Tests in Docker

A Docker container preconfigured to run Jepsen tests is available at `tjake/jepsen` on [Docker Hub](https://hub.docker.com/r/tjake/jepsen). Since it runs Docker inside Docker, it must be run with the privileged flag. A command like `docker run -it --privileged -v /home/jkni/git:/jkni-git tjake/jepsen` will start the container and attach to it as an interactive shell. Since you'll likely be running a newer version of Jepsen/C* tests than those available in the image, you'll want to share the directory containing your local Jepsen/C* clone with the container as in the example above.

## Troubleshooting

If you have trouble getting Scylla to start, check the node logs, and look at
`service scylla-server status` on a DB node.

You may need to up aio-max-nr, either on individual DB nodes, or, for
containers, on the container host.

```
echo 16777216 >/proc/sys/fs/aio-max-nr
```
