# Example usage of multiple publishers

This docker-compose environment intends to illustrate the basic functionality
of `botlnek`.

* A single REST API (with in-memory store and dumb HTTP subscription interface) acts as the server
* Multiple data sources publish information to domains within that REST backend
* A single subscriber receives and pretty-prints the partition events as mutations accrue in the server

# Subscription

A subscriber will receive JSON partition messages whenever new information arrives for a partition.

Because we have multiple sources accruing to each partition (described below), we expect output from the sink to show partition messages that get larger over the lifespan of the partition.

The sink application is pretty simple:

* Turn off IO buffering (for bash pipeline reasons)
* Make a long-lived HTTP request to our endpoint using curl
* Pipe the output stream to `jq` to pretty-print the JSON.

Dirt simple.

# `source` application

The `source` go application emits minimal source registrations to the server.

A given source invocation is handed:
* The HTTP endpoint of the server
* The domain key to which partition/sources are posted
* The `token` to which the sources are posted (which is the namespace of the sources within a partition).
* The time duration used for determining the partition key from the present time.
* The time duration used for determining the source key from the present time.

The source will check the clock every second, and truncate that time down to a partition time and a source time based on the given time durations above.

Each time the partition time and/or source time changes from this, the app registers a new source for the partition time / source time pair.

# Data sources

There are four data sources, with two domains (two sources per domain).  Each is simply an application of the `source` tool described above.

A "domain" is effectively a namespace of similar partitions over time.

All things publishing to a domain must agree on the partition key scheme for that domain, as the registration of a source is to a specific partition key.

See the `docker-compose.yml`, and you can see the publishers are arranged thus:

* `publisher-a-a` and `publisher-a-b` both publish to domain `dom-a`, with a partition scheme organized by minute.
* `publisher-b-a` and `publisher-b-b` both publish to domain `dom-b`, with a partition scheme organized by 45-second intervals.

## `publisher-a-*`

As mentioned, the domain used by these will use a partition based on 60-second intervals.

The `publisher-a-a` uses a 20-second interval for its sources, while `publisher-a-b` uses a 30-second interval.

Thus, within a given minute, we expect to see up to five different representations of the minute's partition, as sources accumulate from each publisher.  Ultimately, we expect 3 sources from `publisher-a-a`, and 2 sources from `publisher-a-b`.

## `publisher-b-*`

As mentioned, the domain used by these partitions based on 45-second intervals.  Thus we expect 4 partitions every 3 minutes (evenly distributed).

These publishers are interesting in that they do not align their source durations with that of the partition duration.

The `publisher-b-a` uses a 30-second interval, which will sometimes result in a single partition getting two such sources and in other cases a partition getting only 1 such source.

The `publisher-b-b` uses a 40-second interval, which will also have the stated phenomenon (some partitions get 1, others get 2).

