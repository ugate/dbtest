const DB = require('./db');
const conf = require('./dbconf');

const db = new DB(conf, null, null, (err) => {
  if (err) return console.error(err);
  console.log('DB ready');

  // assumes the project has a test/sysdate.sql file present
  db.conn1.test.sysdate({ b: 1 }, null, null, function testDbCb(err, rslt) {
    if (err) return console.error(err);
    console.dir(rslt);
  });
});
