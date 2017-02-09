# A Filesystem Backend for Heroic

This module contains a filesystem-based backend for Heroic which will eventually be suitable for
small to medium scale original storage, or as a distributed write-through cache for a larger
installation.

## TODO

* **DONE** Let the `Wal` decide how many transactions should be persisted, and offload memory usage by using
  a `SegmentIterator` over mmap:ed segments of the transaction logs instead of storing all of them
  on heap. This would also simplify rate limiting for how quickly things should be written to disk.
* Garbage collection. This should also act as policies that can be applied for incoming requests,
  or metrics recovered from the `Wal` as well.
  See [Garbage Collection][gc] below.
* Compaction. A must-have feature for long-term retention.
  This would allow the merging of smaller data files into larger ones to reduce filesystem
  overhead. It could also allow for more efficient scanning of data by adding indexes to the
  beginning of the larger files indicating offsets for certain ranges.

[gc]: #garbage-collection

## Testing

You can try out this feature by using the `fs` profile:

```bash
$> tools/heroic-shell -P fs -X fs.storagePath=storage -X fs.compression=gorilla
heroic> load-generated
...
heroic> query average by role where what=disk-used
```

## Filesystem Layout

In the paths described below, `<storage>` refers to the configured storage location.

### Data (`<storage>/data/<xx>/<id>_<width>_<base>[.<ext>]`)

Contains all data stored on this node.

`<xx>` is the first two utf-8 encoded characters in the `<id>`.
This avoids directories becoming to large.

`<id>` is the identifier of the data stored in the file.
This typically corresponds to the hash of the time series stored.

`<width>` is how many seconds wide each segment of a given time series is.
For one hour, this would be `3600000` encoded in base 16 and padded with zeroes to 16 characters
(`000000000036ee80`).

`<base>` is what the lowest timestamp in milliseconds this segment belongs to to.
It must always be evenly divisible by the given `<width>`.
It is encoded the same way as the `<width>`.

An `<ext>` might be present to indicate a different file format.
This is currently used with Gorilla compression (`.tsc`).

These files are periodically flushed from the transactions stored in the transaction log.

### Transaction Logs (`<storage>/wal`)

If enabled, contains append-only logs which maintains the last commited data before an operation is
considered successful.

If this directory contains files while the service is stopped it indicates a non-clean shutdown.
During the next startup of the service, these files will be read to replay operations that might
not have been properly persisted.

#### Transaction Log Entries (`<storage>/wal/log_<id>`)

A single log entry for the transaction log.

These are read in order, where the `<id>` is an ascending hex-encoded number.

Each log entry contains the following data:

```
<size> - 4 bytes, fixed-length encoded size of the entry
<tx-id> - 4 bytes, fixed-length transaction id
<bytes> - bytes corresponding to the given <size> - 4
<checksum> - 4 bytes, CRC32 checksum of <tx-id> + <bytes>
...
```

`<size>` is used to determine the size of the entry, and to detect when a file underflows because
of an incomplete flush.

`<checksum>` is used to determine when an entry has been correctly persisted.

#### Last TxId (`<storage>/wal/id`)

Stores the last _committed_ TxId and its checksum.

A _committed_ TxId is one where it has been verified that the given ID has been persisted to data
directories.

### Garbage Collection

Garbage collections scans over persisted files and removes the ones matching a given policy.

If a **token range** is configured for the current node, any data that falls _outside_ of that
range is deleted.

If an **interval** is configured for the current node, any segments that fall _outside_ of the
configured interval is periodically deleted.

## Distributed Write-Through Cache

Each node is designated either a leader, or a follower.

The cluster configuration contains a list of both, their role in metadata determines what kind of
node they are.
`fs::leader` for leaders, and `fs::follower` for followers.

Leaders maintain the regular transaction log, including all the transactions received over a given
timespan.
Followers maintain a special transaction log that maps each leader transaction, to a local
txId, this is then used to apply operations to the local filesystem backend.

#### Series Id Key Space

The _Key Space_ is a range of all possible time series ids `[0, 2^128]`.

Each follower belongs to a certain range in the key space determined by its **token range**.
This affects which series ids a follower will be able to serve.

A **token range** contains a `start` and `end` 128-bit token. `start` is _inclusive_ and `end` is
_exclusive_. This range _rolls around_ the 128-bit value space, so a `start` value of `42`, and an
`end` value of `0` would cover the closed interval `[42, 2^128]`.

#### Data Stream

Endpoint: `metrics:streamData(Request) -> Message`

The request is initiated by the follower, and contains the last recorded leader `txId`.

The leader will then send all the transactions that occurred, after the provide `txId` as a stream
of operations.
If `txId` is omitted, all stored transactions will be sent as a state transfer.
The request also contains a `tokenRange`, which will act as a filter applied by the leader.
Only transactions with IDs matching the given range will be streamed.

##### `SegmentSnapshot`

`SegmentSnapshot` is a special kind of message streamed that is sent when the `txId` is omitted
from the request.
It contains a snapshot of the segment being transmitted in its raw form.
The metadata includes the `id`, `width`, `base` of the segment it belongs to, and any compression
that is being used.
The data includes the last `txId` that modified this snapshot and all data that belongs to it and
the raw data of the snapshot.
A received snapshot is merged into the local snapshot of the corresponding id.

##### `ApplyTransaction`

After snapshotting has completed, or if that is not necessary, a stream of `ApplyTransaction`
messages will be sent to the follower corresponding to
transactions that has been applied to the leader.
This message contains the `txId` of the transaction and the content of the transaction.

##### `Ping`

At a certain **ping interval**, `Ping` is sent over the channel to check that it is alive.
The ping is a special type of transaction that has its own `txId`.
Because the transactions are filtered, this is also used to increase the TxId as observed by the
follower.
If a ping is not received at the expected **ping interval**, the leader is considered dead, and the
follower closes its channel.
The follower will then attempt to re-establish this channel to the leader.

This message contains the special `lagMillis`, which is how many milliseconds (in leader-local
time) behind the current stream of transactions is.
This must be used by the follower to determine how _recent_ its data is.

#### Follower Lag

At any given time, the global `lagMillis` is the value as recorded in of the most recently received
ping, plus the **ping interval** and the number of **milliseconds since the last ping** was
received.
This indicates the _worst case lag_ behind a follower can be, and should be used to determine if
the follower can respond to requests with certain timeliness guarantees.

## How To

#### Add a Leader

Adding a leader is as simple as assigning the role `fs::leader` to a node.
Writes must be directed _only_ to leaders.

#### Add a Follower

Adding a follower is as simple as assigning the role `fs::follower` to a node.

#### Splitting the Key Space

Reconfigure new followers with a **token range** that includes the split ranges that you wish to
increase coverage for. When these are done, they will start receiving requests for the new token
range and you can decrease the ranges for the old replicas and restart their nodes.
