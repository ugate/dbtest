'use strict';

const Fs = require('fs');
const Path = require('path');
const Util = require('util');

/**
* Oracle database implementation
* TODO: oracledb requires native builds {@link https://github.com/oracle/node-oracledb/issues/141}
*/
module.exports = class OracleDB {

  /**
   * Constructor
   * @constructs OracleDB
   * @arg {String} un the connection username
   * @arg {String} up the connection password
   * @arg {Object} gopts the global options
   * @arg {String} gopts.host the host name
   * @arg {Integer} [gopts.port] the port
   * @arg {String} [gopts.protocol=TCP] the protocol
   * @arg {Object} [gopts.dialectOptions={}] the options passed as directly into the `oracledb` configuration
   * @arg {Object} [gopts.pool] the pooling options
   * @arg {Integer} [gopts.pool.min] the minimum number of connections
   * @arg {Integer} [gopts.pool.max] the maximum number of connections
   * @arg {Integer} [gopts.pool.idle] the number of seconds after which the pool terminates idle connections
   * @arg {Integer} [gopts.pool.increment] the number of connections that are opened whenever a connection request exceeds the number of currently open connections
   * @arg {Integer} [gopts.pool.timeout] the number of milliseconds that a connection request should wait in the queue before the request is terminated
   * @arg {Integer} [gopts.pool.alias] the alias of this pool in the connection pool cache
   * @arg {String} svc the Oracle service name 
   * @arg {String} sid the Oracle SID
   * @arg {String} [sidDir=process.cwd()] the directory path where the TNS file will be created 
   * @arg {Object} [sidTrack={}] tracking object that will be used to prevent possible file overwrites of the TNS file when multiple {@link OracleDB}s are used
   * @arg {function} [logger=console.log] the logging function 
   * @arg {Boolean} [debug] the flag that indicates when debugging is turned on
   */
  constructor(un, up, gopts, svc, sid, sidDir = process.cwd(), sidTrack = {}, logger = console.log, debug) {
    const ora = internal(this);

    ora.at.oracledb = require('oracledb');
    ora.at.oracledb.connectionClass = gopts.connectionClass || 'DBPOOL';
    ora.at.oracledb.Promise = Promise; // tell Oracle to use the built-in promise

    ora.at.queryTypes = { SELECT: 'select' };
    ora.at.logger = logger;
    ora.at.debug = debug;
    ora.at.pool = { conf: gopts.dialectOptions || {}, src: null };
    ora.at.gopts = gopts;

    ora.at.pool.conf.user = un;
    ora.at.pool.conf.password = up;
    ora.at.meta = { connections: { open: 0, inUse: 0 } };

    if (sid) {
      process.env.TNS_ADMIN = sidDir;
      const fpth = Path.join(process.env.TNS_ADMIN, 'tnsnames.ora');
      const fdta = `${sid} = (DESCRIPTION = (ADDRESS = (PROTOCOL = ${gopts.protocol || 'TCP'})(HOST = ${gopts.host})(PORT = ${gopts.port || 1521}))` +
        `(CONNECT_DATA = (SERVER = POOLED)(SID = ${sid})))${require('os').EOL}`;
      if (typeof sidTrack.tnsCnt === 'undefined' && (sidTrack.tnsCnt = 1)) {
        Fs.writeFileSync(fpth, fdta);
      } else if (++sidTrack.tnsCnt) {
        Fs.appendFileSync(fpth, fdta);
      }
      ora.at.pool.conf.connectString = sid;
      ora.at.connectionType = 'SID';
    } else {
      ora.at.pool.conf.connectString = `${gopts.host}${(svc && ('/' + svc)) || ''}${(gopts.port && (':' + gopts.port)) || ''}`;
      ora.at.connectionType = 'Service';
    }
    ora.at.pool.conf.poolMin = gopts.pool && gopts.pool.min;
    ora.at.pool.conf.poolMax = gopts.pool && gopts.pool.max;
    ora.at.pool.conf.poolTimeout = gopts.pool && gopts.pool.idle;
    ora.at.pool.conf.poolIncrement = gopts.pool && gopts.pool.increment;
    ora.at.pool.conf.queueTimeout = gopts.pool && gopts.pool.timeout;
    ora.at.pool.conf.poolAlias = gopts.pool && gopts.pool.alias;
  }

  /**
   * Initializes OracleDB by creating the connection pool
   * @arg {Object} [opts] initialization options
   * @arg {Integer} [opts.numOfPreparedStmts] the number of prepared SQL statements
   * @arg {function} [cb] callback function(err) 
   */
  init(opts, cb) {
    const ora = internal(this), numSql = (opts && opts.numOfPreparedStmts && opts.numOfPreparedStmts) || 0;
    // statement cache should account for the number of prepared SQL statements/files by a factor of 3x to accomodate up to 3x fragments for each SQL file
    ora.at.pool.conf.stmtCacheSize = (numSql * 3) || 30;
    ora.at.oracledb.createPool(ora.at.pool.conf).then(function oraPoolCb(oraPool) {
      ora.at.pool.alias = oraPool.poolAlias;
      ora.at.logger(`Oracle ${ora.at.connectionType} connection pool "${oraPool.poolAlias}" created with poolPingInterval=${oraPool.poolPingInterval} ` +
        `stmtCacheSize=${oraPool.stmtCacheSize} (${numSql} SQL files) poolTimeout=${oraPool.poolTimeout} poolIncrement=${oraPool.poolIncrement} ` +
        `poolMin=${oraPool.poolMin} poolMax=${oraPool.poolMax}`);
      if (cb) cb(null, oraPool);
    }).catch(function oraPoolCreateError(err) {
      ora.at.logger(`Unable to create Oracle connection pool ${Util.inspect(err)}`);
      if (cb) cb(err);
    });
  }

  /**
   * Executes a SQL statement
   * @arg {String} sql the SQL to execute 
   * @arg {Object} [opts] the options that control SQL execution
   * @arg {Object} [opts.replacements] the key/value pair of replacement parameters that will be used in the SQL
   */
  query(sql, opts) {
    const ora = internal(this);
    const pool = ora.at.oracledb.getPool(ora.at.pool.alias);
    return new Promise(function oraQueryPromise(resolve, reject) {
      return pool.getConnection().then(function oraConnThen(conn) {
        ora.at.meta.connections.open = pool.connectionsOpen;
        ora.at.meta.connections.inUse = pool.connectionsInUse;
        // becasue replacements may be altered for SQL a clone is made
        const bndp = (opts && opts.replacements && JSON.parse(JSON.stringify(opts.replacements))) || {}, oopts = { outFormat: ora.at.oracledb.OBJECT };

        // TODO: oracle will throw "ORA-01036: illegal variable name/number" when unused bind parameters are passed
        for (var prop in bndp) {
          if (sql.indexOf(`:${prop}`) < 0) delete bndp[prop];
        }

        return conn.execute(sql, bndp, oopts).then(function oraResults(rslts) {
          resolve(rslts && rslts.rows);
          return conn.close();
        }).catch(function oraQueryError(err) {
          err.sql = sql;
          err.sqlBindParams = bndp;
          reject(err);
          return conn.close();
        });
      }).catch(function oraGetPoolConnError(err) {
        err.sql = sql;
        err.sqlOptions = opts;
        reject(err);
      });
    });
  }

  /**
   * Closes the connection pool
   */
  close() {
    const ora = internal(this), pool = ora.at.oracledb.getPool(ora.at.pool.alias);
    ora.at.logger(`Closing Oracle connection pool "${ora.at.pool.alias}"`);
    return pool ? pool.close() : null;
  }

  /**
   * Oracle specific query types
   */
  get QueryTypes() {
    return internal(this).at.queryTypes;
  }

  /**
   * @returns {Integer} the last captured number of connections
   */
  get lastConnectionCount() {
    return internal(this).at.meta.connections.open;
  }

  /**
   * @returns {Integer} the last captured number of connections that were in use
   */
  get lastConnectionInUseCount() {
    return internal(this).at.meta.connections.inUse;
  }
};

// private mapping
let map = new WeakMap();
let internal = function(object) {
  if (!map.has(object)) {
    map.set(object, {});
  }
  return {
    at: map.get(object),
    this: object
  };
};
