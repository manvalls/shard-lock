var path = require('path'),
    walk = require('y-walk'),
    Cb = require('y-callback'),
    NCb = require('y-callback/node'),
    wait = require('y-timers/wait'),
    Resolver = require('y-resolver'),
    {Promise: ZooKeeper, ZOO_SEQUENCE, ZOO_EPHEMERAL, ZNONODE, ZBADVERSION, ZNODEEXISTS, ZNOTEMPTY} = require('zookeeper'),

    zk = Symbol(),
    rq = Symbol(),
    inited = Symbol(),
    options = Symbol(),
    lostCb = Symbol(),
    node = Symbol(),
    bp = Symbol(),
    gp = Symbol(),
    wt = Symbol(),
    chl = Symbol(),
    it = Symbol(),
    au = Symbol(),

    acquire, release, requested, lost, check, ack,
    initZk, getGroups;

class ShardLock{

  constructor({ connect, timeout, debug_level, host_order_deterministic, wait_time = 500, init_timeout , auth = {}} = {}){

    this[wt] = wait_time;
    this[it] = init_timeout;
    this[au] = auth;
    this[options] = { connect, timeout, debug_level, host_order_deterministic };

    let init = (...args) => {

      if(this[zk]){
        this[zk].removeListener('error', init);
        this[zk].removeListener('close', init);
      }

      this[zk] = new ZooKeeper();
      this[zk].once('error', init);
      this[zk].once('close', init);

      this[inited] = false;

    };

    init();

  }

  acquire(){
    var shard = new Shard();
    return wrap(
      acquire.call(this, shard, ...arguments),
      shard
    );
  }

  close(){
    this[zk].close();
  }

}

class Shard{

  constructor(){

    this[lostCb] = Cb(() => {
      this[zk].removeListener('error', this[lostCb]);
      this[zk].removeListener('close', this[lostCb]);
    });

  }

  release(){
    return wrap(
      release.apply(this, arguments),
      this,
      true
    );
  }

  ack(){
    return wrap(
      ack.apply(this, arguments),
      this,
      true
    );
  }

  requested(){
    return this[rq] = this[rq] || wrap(
      requested.apply(this, arguments),
      this,
      true
    );
  }

  check(){
    return wrap(
      check.apply(this, arguments),
      this,
      true
    );
  }

  lost(){
    return lost.apply(this, arguments);
  }

}

// Class methods

function wrap(yd, shard, accept){
  var res = new Resolver();

  yd.throws = false;
  res.bind(yd);

  if(accept) res.bind(shard.lost());
  else{

    shard.lost().listen(res.reject, [new Error('The lock was lost')], res);

    shard.lost().add(
      yd.listen(wrappedListener, [shard])
    );

  }

  return res.yielded;
}

function wrappedListener(shard){
  if(this.rejected && !shard.lost().done) shard[lostCb]();
}

acquire = walk.wrap(function*(shard, basePath = '/shard-lock'){
  var prefix = path.resolve(basePath, 'group-'),
      processing, awaiting, data, current,
      group, groupPath, nodePath, nodeName,
      change, children, childrenChange;

  shard[zk] = this[zk];
  shard[bp] = shard.path = basePath;
  yield initZk(this, shard);

  do{

    let nodePrefix;

    if(nodePath) yield this[zk].delete_(nodePath, -1);
    ({ processing, awaiting } = yield getGroups(this[zk], basePath));

    while(!awaiting.length){

      try{
        yield this[zk].create(prefix, '', ZOO_SEQUENCE);
      }catch(err){
        if(err != ZNONODE) throw err;
      }

      ({ processing, awaiting } = yield getGroups(this[zk], basePath));

    }

    group = awaiting[0];
    nodePrefix = path.resolve(basePath, group, 'node-');

    try{
      nodePath = yield this[zk].create(nodePrefix, '', ZOO_SEQUENCE | ZOO_EPHEMERAL);
    }catch(err){
      if(err != ZNONODE) throw err;
      nodePath = null;
      continue;
    }

  }while(!nodePath);

  groupPath = path.resolve(basePath, group);

  while(processing.length && group != processing[0]){
    let groupPath = path.resolve(basePath, processing[0]);

    // Loop until the current processing node is empty

    while(true){
      let stat, childPath, cb, children;

      try{
        children = yield this[zk].get_children(groupPath, false);
      }catch(err){
        if(err != ZNONODE) throw err;
        break;
      }

      children = children.filter(child => child.indexOf('node-') != -1);
      if(!children.length) break;
      childPath = path.resolve(groupPath, children[0]);

      // Loop until the first children of the group is gone

      do{
        yield cb;
        cb = Cb();

        try{
          stat = yield this[zk].w_exists(childPath, cb);
        }catch(err){
          if(err != ZNONODE) throw err;
          stat = null;
        }

      }while(stat);

    }

    ({ processing, awaiting } = yield getGroups(this[zk], basePath));
  }

  shard[gp] = groupPath;
  shard[node] = nodePath;
  nodeName = nodePath.split('/').slice(-1)[0];

  while(group != processing[0]){
    let children;

    yield {change, childrenChange};

    childrenChange = null;
    children = yield this[zk].get_children(groupPath, false);
    children = children.filter(child => child.indexOf('node-') != -1);
    children.sort();

    if(children[0] == nodeName){
      let stat;

      yield wait(this[wt]);
      childrenChange = Cb();
      [children, stat] = yield this[zk].w_get_children2(groupPath, childrenChange);
      children = children .filter(child => child.indexOf('node-') != -1)
                          .sort()
                          .map(getPosition);

      children = new Int32Array(children);
      children = new Buffer(children.buffer);

      if(!stat.dataLength){
        try{
          yield this[zk].set(groupPath, children, stat.version);
        }catch(err){
          if(err != ZBADVERSION) throw err;
        }
      }

    }else{
      let p = path.resolve(groupPath, children[0]),
          stat;

      childrenChange = Cb();

      try{
        stat = yield this[zk].w_exists(p, childrenChange);
      }catch(err){
        if(err != ZNONODE) throw err;
        stat = null;
      }

      if(!stat) childrenChange = null;
    }

    change = Cb();
    [,data] = yield this[zk].w_get(groupPath, change);
    ({ processing, awaiting } = yield getGroups(this[zk], basePath));

  }

  children = new Int32Array(data.buffer);
  shard[chl] = children.length;

  current = children.indexOf(getPosition(nodeName));

  if(current == -1){
    yield shard.release();
    return yield this.acquire(basePath);
  }

  shard.from = current / children.length;
  shard.to = (current + 1) / children.length;
  return shard;
});

release = walk.wrap(function*(){

  try{
    yield this[zk].delete_(this[node], -1);
  }catch(err){
    if(err != ZNONODE) throw err;
  }finally{
    this[lostCb]();
  }

});

ack = walk.wrap(function*(){
  var ackPath = path.resolve(this[gp], 'ack-');

  try{
    yield this[zk].create(ackPath, '', ZOO_SEQUENCE);
  }catch(err){
    if(err != ZNONODE) throw err;
  }finally{
    yield this.release();
  }

});

requested = walk.wrap(function*(){
  var change, schange, res;

  while(true){
    let awaiting, siblings;

    res = yield { change, schange };

    if(!schange || 'schange' in res){

      try{
        schange = Cb();
        siblings = yield this[zk].w_get_children(this[gp], schange);
        if(siblings.length < this[chl]) return;
      }catch(err){
        if(err != ZNONODE) throw err;
        return;
      }

    }

    if(!change || 'change' in res){
      ({ awaiting, change } = yield getGroups(this[zk], this[bp], true));
      if(awaiting.length) return;
    }

  }

});

check = walk.wrap(function*(){
  let awaiting, siblings;

  try{
    siblings = yield this[zk].get_children(this[gp], false);
    if(siblings.length < this[chl]) return true;
  }catch(err){
    if(err != ZNONODE) throw err;
    return true;
  }

  ({ awaiting } = yield getGroups(this[zk], this[bp]));
  if(awaiting.length) return true;

  return false;
});

lost = walk.wrap(function*(){
  yield this[lostCb];
  return true;
});

// Protocol methods

initZk = walk.wrap(function*(shardLock, shard){

  shard[zk].once('error', shard[lostCb]);
  shard[zk].once('close', shard[lostCb]);

  if(!shardLock[inited]){
    let awaitCb;

    if(shardLock[it]){
      let w = wait(shardLock[it]);

      awaitCb = cb => {
        Resolver.Yielded.get(cb).throws = false;
        return {cb, w};
      };

    }else{
      awaitCb = cb => cb;
    }

    let cb = NCb();
    shardLock[zk].connect(shardLock[options], cb);
    yield awaitCb(cb);

    for(let key of Object.keys(shardLock[au])){
      let cb = NCb();
      shardLock[zk].add_auth(key, shardLock[au][key], cb);
      yield awaitCb(cb);
    }

    shardLock[inited] = true;
  }

});

getGroups = walk.wrap(function*(zk, basePath, watch){
  var awaiting = [],
      processing = [],
      created = false,
      names, groups, info, change;

  try{

    if(watch){
      change = Cb();
      names = yield zk.w_get_children(basePath, change);
    }else names = yield zk.get_children(basePath, false);

  }catch(err){
    if(err != ZNONODE) throw err;

    try{
      yield zk.create(basePath, '', 0);
    }catch(err){
      if(err != ZNODEEXISTS) throw err;
    }

    names = [];
    change = null;
    created = true;
  }

  names.sort();
  groups = names.map(name => path.resolve(basePath, name));

  try{
    info = yield groups.map(group => zk.get_children2(group, false));
  }catch(err){
    for(let error of err.errors) if(error && error != ZNONODE) throw error;
    info = [];
  }

  try{

    yield groups.filter((g, i) => info[i] != null).map((p, i) => {
      let [children, stat] = info[i],
          normalChildren = children.filter(child => child.indexOf('node-') != -1),
          acks = children.filter(child => child.indexOf('ack-') != -1),
          name = names[i];

      if(stat){
        if(stat.dataLength > 0 && normalChildren.length != 0) processing.push(name);
        else if(stat.dataLength == 0 && !awaiting.length) awaiting.push(name);
        else{
          let deletes = [];
          for(let ack of acks) deletes.push(
            zk.delete_(path.resolve(p, ack), -1)
          );

          deletes.push(zk.delete_(p, -1));
          return deletes;
        }
      }

    });

  }catch(err){
    for(let error of err.errors){
      if(error && error.errors){
        for(let err of error.errors){
          if(err && err != ZNONODE && err != ZNOTEMPTY) throw err;
        }
      }
    }
  }

  if(!(created || processing.length || awaiting.length)){

    try{

      // Recreate the base path to avoid overflows

      yield [
        zk.delete_(basePath, -1),
        zk.create(basePath, '', 0)
      ];

    }catch(err){

      for(let error of err.errors) if(error && error != ZNONODE && error != ZNODEEXISTS && error != ZNOTEMPTY){
        throw error;
      }

    }

  }

  return { processing, awaiting, change };
});

function getPosition(path){
  var position = path.split('-').slice(-1)[0];
  return parseInt(position);
}

/*/ exports /*/

module.exports = ShardLock;
