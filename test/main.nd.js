var t = require('u-test'),
    assert = require('assert'),
    wait = require('y-timers/wait'),
    ShardLock = require('../main');

t('Basic sharding', function*(){
  var sl = new ShardLock({
    connect: 'localhost:2181',
    timeout: 2000
  });

  var N = 4;
  var shards = [];
  var shards2 = [];

  // Cleaning step

  var requested;

  do{
    let shard = yield sl.acquire('/test1');
    requested = yield shard.check();
    yield shard.release();
  }while(requested);

  for(let i = 0;i < N;i++) shards[i] = sl.acquire('/test1');
  shards = yield shards;
  for(let i = 0;i < N;i++) shards2[i] = sl.acquire('/test1');

  for(let i = 0;i < N;i++){
    let shard = shards[i];

    assert.strictEqual(shard.from, i / N);
    assert.strictEqual(shard.to, (i + 1) / N);
  }

  yield wait(500);
  for(let i = 0;i < N;i++) yield shards[i].requested();
  for(let i = 0;i < N;i++) yield shards[i].ack();
  for(let i = 0;i < N;i++) yield shards[i].lost();

  shards2 = yield shards2;

  for(let i = 0;i < N;i++){
    let shard = shards2[i];

    assert.strictEqual(shard.from, i / N);
    assert.strictEqual(shard.to, (i + 1) / N);
  }

  for(let i = 0;i < N;i++) yield shards2[i].release();
  for(let i = 0;i < N;i++) yield shards2[i].lost();
  for(let i = 0;i < N;i++) yield shards2[i].requested();

  for(let i = 0;i < N;i++) shards[i] = sl.acquire('/test1');
  shards = yield shards;

  for(let i = 0;i < N;i++){
    let shard = shards[i];

    assert.strictEqual(shard.from, i / N);
    assert.strictEqual(shard.to, (i + 1) / N);
  }

  sl.close();
  for(let i = 0;i < N;i++) yield shards[i].lost();
  for(let i = 0;i < N;i++) yield shards[i].requested();

});

t('Leader takeover', function*(){
  var s1 = new ShardLock({
    connect: 'localhost:2181',
    timeout: 2000
  });

  var s2 = new ShardLock({
    connect: 'localhost:2181',
    timeout: 2000
  });

  // Cleaning step

  var requested;

  do{
    let shard = yield s1.acquire('/test2');
    requested = yield shard.check();
    yield shard.release();
  }while(requested);

  var N = 4;
  var shards = [];
  var shards2 = [];

  for(let i = 0;i < N;i++) shards[i] = s1.acquire('/test2');
  yield wait(300);
  for(let i = 0;i < N;i++) shards2[i] = s2.acquire('/test2');

  s1.close();

  try{ yield shards; }
  catch(err){ }

  shards2 = yield shards2;

  for(let i = 0;i < N;i++){
    let shard = shards2[i];

    assert.strictEqual(shard.from, (N + i) / (N * 2));
    assert.strictEqual(shard.to, (N + i + 1) / (N * 2));
  }

  for(let i = 0;i < N;i++) yield shards2[i].requested();
  for(let i = 0;i < N;i++) yield shards2[i].release();
  for(let i = 0;i < N;i++) yield shards2[i].lost();

  s2.close();

});

t('shard.ack() vs shard.release()', function*(){
  var sl = new ShardLock({
    connect: 'localhost:2181',
    timeout: 2000
  });

  var [shard1, shard2, shard3] = yield [
    sl.acquire('/test3'),
    sl.acquire('/test3'),
    sl.acquire('/test3')
  ];

  assert(!(yield shard1.check()));
  assert(!(yield shard2.check()));
  assert(!(yield shard3.check()));

  yield shard1.ack();

  assert(yield shard1.check());
  assert(!(yield shard2.check()));
  assert(!(yield shard3.check()));

  yield shard2.release();

  assert(yield shard1.check());
  assert(yield shard2.check());
  assert(yield shard3.check());

  sl.close();

  yield shard1.lost();
  yield shard2.lost();
  yield shard3.lost();

});
