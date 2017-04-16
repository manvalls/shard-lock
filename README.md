# ShardLock [![Build Status][ci-img]][ci-url] [![Coverage Status][cover-img]][cover-url]

ShardLock is a protocol backed by Apache ZooKeeper which allows each node to acquire a lock on a certain partition of the system. It is designed to help with parallel batch processing.

ShardLock assumes that your system can be splitted in arbitray partitions defined by *start* and *end* positions. These positions are specified as fractions of the system.

As an example, let's suppose you need to write a distributed batch process which does some work with each row of a certain database table. We would:

- Add a secondary index to the table called *position* and assign a number in the `[0,1)` range to each row
- Spin up any number of workers
- Inside each worker:
    - Use ShardLock to get the assigned partition of the system
    - Get the rows the position of which falls inside the acquired partition
    - Process all assigned rows
    - Check if resharding is needed
    - Release the partition lock with an ACK
    - Repeat the process until resharding is not needed

## new ShardLock( options )

```javascript
var ShardLock = require('shard-lock'),
    sl = new ShardLock({
      connect: 'localhost:2181',
      wait_time: 1000
    });
```

The ShardLock constructor accepts all options supported by [zookeeper](https://www.npmjs.com/package/zookeeper), used for establishing the connection, plus the following protocol-specific options:

- `wait_time`: the number of milliseconds to wait before acquiring the lock, 500 by default
- `init_timeout`: the number of milliseconds to wait before giving up while trying to connect to ZooKeeper

## shardLock.close( )

```javascript
sl.close();
```

Closes the current ZooKeeper connection.

## shardLock.acquire( path )

```javascript
sl.acquire('/system').then( shard => {
  console.log(`
    Lock acquired over ${shard.path},
    in the interval [ ${shard.from}, ${shard.to} )
  `);
} );
```

Acquires the lock over a partition of the given path. It is assumed that no protocol-extraneus activity happens at the provided path. It is not necessary that the given path exists, but it must be inside an existing folder, i.e for `/parent/child` a znode at `/parent` must exist or the acquisition will fail.

Parameters:

- `path`: a ZooKeeper path

Returns a promise which will be resolved to a Shard instance.

## shard.from

A number in the `[0,1)` range. It represents the start of the partition the lock of which was acquired. It is an **including** endpoint.

## shard.to

A number in the `(0,1]` range. It represents the end of the partition the lock of which was acquired. It is an **excluding** endpoint.

## shard.path

The ZooKeeper path this shard is part of.

## shard.release( )

```javascript
shard.release().then( () => {
  // The lock was correctly released
} );
```

Releases the lock. Returns a promise.

## shard.ack( )

```javascript
shard.ack().then( () => {
  // The lock was correctly released
} );
```

Releases the lock. Unlike `shard.release()` it doesn't trigger the resharding process. Returns a promise.

## shard.check( )

```javascript
shard.check().then((requested) => {
  if(requested) console.log('Resharding is needed');
  else console.log('Resharding is not needed');
});
```

Returns a promise which resolves to a boolean. If it's `true`, resharding may be needed.

## shard.requested( )

```javascript
shard.requested().then(() => {
  // Resharding is needed
});
```

Returns a promise which will be resolved when resharding is needed, e.g when a new node is added to the system or a previous one gets removed.

## shard.lost( )

```javascript
shard.lost().then(() => {
  // The lock was lost
});
```

Returns a promise which will be resolved when the lock was lost, e.g due to a network error.

[ci-img]: https://circleci.com/gh/manvalls/shard-lock.svg?style=shield
[ci-url]: https://circleci.com/gh/manvalls/shard-lock
[cover-img]: https://coveralls.io/repos/manvalls/shard-lock/badge.svg?branch=master&service=github
[cover-url]: https://coveralls.io/github/manvalls/shard-lock?branch=master
