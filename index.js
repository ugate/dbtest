const DB = require('./db');
const conf = require('./dbconf');
const cache = {
  method: (cacheFuncName, funcToCache, cacheOptions) => {
    // typically the cache would be soemthing more robust like Redis, etc.
    this[cacheFuncName] = funcToCache;
  },
  methods: {}
};

const db = new DB(conf, cache, null, (err) => {
  if (err) return console.error(err);
  console.log('Executing test SQL');

  // when the DB connection defined in dbconf.json has a name the path
  // uses the "name" value in place of the actual value
  // in this case it would have been db.test.sysdate
  // this allows the same SQL files to be used in multiple connections
  db.conn1.sysdate({ b: 1 }, null, null, function testDb1Cb(err, rslt) {
    if (err) return console.error(err);
    console.log('conn1 sysdate test SQL result:');
    console.dir(rslt);
  });
  db.conn1.client.version({ t: 'SID' }, null, null, function testDb2Cb(err, rslt) {
    if (err) return console.error(err);
    console.log('conn1 client version test SQL result:');
    console.dir(rslt);
  });
});
