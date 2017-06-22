'use strict';

const OracleDB = require('./oracle');
const Sequelize = require('sequelize');
const Fs = require('fs');
const Path = require('path');
const compare = Object.freeze({
  '=': function eq(x, y) { return x === y; },
  '<': function lt(x, y) { return x < y; },
  '>': function gt(x, y) { return x > y; },
  '<=': function lteq(x, y) { return x <= y; },
  '>=': function gteq(x, y) { return x >= y; },
  '<>': function noteq(x, y) { return x !== y; }
});

/**
 * Cache manager responsible for registering and calling cached functions
 *
 * @typedef {Object} Cache
 * @property {function} method the function(id, cb) that calls a registered cached function and, when complete, executes the callback function(options, completeCb) with the "completeCb" function that takes
 * an "error" (if any) followed by the cached data
 * @property {Object} [methods={}] read-only method function cache used for internal method assignment
 */

/**
 * A database manager that autogenerates/manages SQL execution functions from underlying SQL statement files. Features include:
 *
 * * Debugging options that allow for near real time updates to SQL files without restarting an application
 * * Autogeneration of object paths that coincide with SQL file paths
 * * Expanded SQL replacement and fragment processing
 * * Unlike strict ORM/API based solutions, models are generated on the fly- lending itself to a more function centric design with minimal overhead and maximum/optimal utilization of SQL syntax and DBA
 * interaction
 *
 * SQL Statement Replacements:
 * Each SQL file can define multiple encapsulators that indicates what portions of an SQL statement will be present before execution takes place. The simplest of which is the typical syntax commonly
 * associated with named parameters within prepared statements. They follow the format `:someParam` where `someParam` is a parameter passed in to the DB managed SQL function `params`. The sysntax can
 * also be replaced by an array of values. For instance, when `{ someParam: ['one','two','three'] }` is passed into the SQL function `params` and the SQL statement contains a `WHERE SOME_COL IN (:someParam)`
 * segment `params` will become `{ someParam: 'one', someParam1: 'two', someParam2: 'three' }` and the SQL segment will become `WHERE SOME_COL IN (:someParam, :someParam1, :someParam2)` and normal named
 * parameter replacement will be applied.
 * The second type of replacement involves SQL statement segments that are fragmented by use case. An example would be where only a portion of the SQL statement will be included when `frags` is passed into
 * the DB managed SQL function that matches a key found in the SQL statement that's surrounded by an open (e.g. `[[? someKey]]`) and closing (i.e. `[[?]]`) fragment definition. Fragment keys cannot be nested.
 * For instance if `frags` is passed into a DB managed SQL function that contains `['someKey']` for a SQL statement segment `WHERE SOME_COL = 'test' [[? someKey]] AND SOME_COL2 IS NOT NULL [[?]]` the resulting
 * SQL statement will become `WHERE SOME_COL = 'test' AND SOME_COL2 IS NOT NULL`. When `frags` is omitted or `frags` contains an array that does not contain a `somekey` value, then the resulting SQL statement
 * segment would become `WHERE SOME_COL = 'test'`.
 * A third type of replacement is dialect specific and allows for SQL files that, for the most part ANSI compliant, but may have slight deviations in syntax that's specific to an individual DB vendor. SQL files
 * can coexist between DB vendors, but segments of the SQL statement will only be included when executed under a DB within a defined dialect. An example would be the use of `SUBSTR` in Oracle versus the ANSI
 * use of `SUBSTRING`. A SQL segment may contain `[[! oracle]] SUBSTR(SOME_COL, 1, 1) [[!]] [[! mssql]] SUBSTRING(SOME_COL FROM 1 FOR 1) [[!]]`, but the result when the dialect is Oracle would become
 * `SUBSTR(SOME_COL, 1, 1)` whereas SQL Server would become `SUBSTRING(SOME_COL FROM 1 FOR 1)`.
 * Sometimes programs connect to DBs that are shared accross one or more applications. Some portions of a program may need to execute SQL statements that are similar in nature, but have some versioning
 * discrepancies between DB instances. Say we have a DB instance for an up-and-coming version that has some modifications made to it's structure, but is not enough to warrent two separate copies of some SQL
 * statements. It may make more sense to maintain one copy of a SQL file/statement and account for the discrepancies within the SQL file. We can do so by encapsulating the SQL segment by surrounding it with an
 * opening `[[version = 1]]` and closing `[[version]]` key (valid version quantifiers can be `=`, `<`, `>`, `<=`, `>=` or `<>`). So, if there were a SQL segment that contained
 * `[[version <= 1]] SOME_OLD_COL [[version]] [[version > 1]] SOME_NEW_COL [[version]]` and the DB manager connection contained a `version` with a value of `1` then the resulting segment would become
 * `SOME_OLD_COL`. Likewise, a `version` of `1.5` would become `SOME_NEW_COL`.
 */
class DB {

  /**
  * Creates a new database manager
  * @arg {Object} conf the configuration
  * @arg {String} [conf.privatePath=process.cwd()] current working directory where generated DB manager files will be generated (if any)
  * @arg {Object} conf.univ the universal DB configuration that, for security and sharing puposes, remains external to an application
  * @arg {Object} conf.univ.db the database configuration that contains connection ID objects that match the connection IDs of each of the conf.db.connections - each connection object should contain a
  * "host", "username" and "password" property that will be used to connect to the underlying DB (e.g. { db: myConnId: { host: "someDbhost.example.com", username: 'someUser', password: 'somePass' } })
  * @arg {Object} conf.db the database configuration
  * @arg {Object[]} conf.db.connections the connections that will be configured
  * @arg {String} conf.db.connections[].id identifies the connection within the passed conf.univ.db
  * @arg {String} conf.db.connections[].name the name given to the database used for logging, the accessible property name in the DB manager and the cwd relative directory used when no dir is defined
  * @arg {String} [conf.db.connections[].dir] the alternative dir where *.sql files will be found and will be accessible in the DB manager by name followed by an object for each name separated by period(s)
  * within the file name with the last entry as the executable function(params, locale, frags, cb) that executes the SQL where "params" is the Object that contains the parameter replacements that will be matched
  * within the SQL, the "locale" String representing the locale that will be used for date parameter replacements, "frags" is an optional String[] of (see replacements section for more details) and a "cb"
  * function(error, results). For example, a connection named "conn1" and a SQL file named "user.team.details.sql" will be accessible within the DB manager as "db.conn1.user.team.details(params, locale, frags, cb)".
  * @arg {Float} [conf.db.connections[].version] a version that can be used for replacement selection within the SQL (see replacements section for more details)
  * @arg {String} [conf.db.connections[].service] the service name defined by the underlying DB (must define if SID is not defined)
  * @arg {String} [conf.db.connections[].sid] the SID defined by the underlying DB (use only when supported, but service is preferred)
  * @arg {Object} [conf.db.connections[].params] global object that contains parameter values that will be included in all SQL calls made under the connection for parameter replacements if not overridden
  * by individual "params" passed into the SQL function
  * @arg {Object} [conf.db.connections[].preparedSql] the object that contains options for prepared SQL
  * @arg {Object} [conf.db.connections[].preparedSql.cache] when a valid "cache" object is passed, the object that defines how caching refreshes will take place for SQL files (CAUTION: NOT FOR PRODUCTION USE)
  * @arg {Integer} [conf.db.connections[].preparedSql.cache.expiresIn] the number of millisecods that the SQL file will be marked as stale and refreshed on a subsiquent call
  * @arg {Integer} [conf.db.connections[].preparedSql.cache.generateTimeout] the number of milliseconds to wait before returning a timeout error when cache retrieval takes too long to return a result
  * @arg {Object} conf.db.connections[].sql the object that contains the SQL connection options (excluding username/password)
  * @arg {String} [conf.db.connections[].sql.host] the DB host override from conf.univ.db
  * @arg {String} conf.db.connections[].sql.dialect the DB dialect (e.g. mysql, mssql, oracle, etc.)
  * @arg {Object} [conf.db.connections[].sql.dialectOptions] options for the specified dialect passed directly into the DB library
  * @arg {Object} [conf.db.connections[].sql.pool] the connection pool options
  * @arg {Integer} [conf.db.connections[].sql.pool.max] the maximum number of connections in the pool
  * @arg {Integer} [conf.db.connections[].sql.pool.min] the minumum number of connections in the pool
  * @arg {Integer} [conf.db.connections[].sql.pool.idle] the maximum time, in milliseconds, that a connection can be idle before being released
  * @arg {String[]} [conf.db.connections[].log] additional logging parameters passed to the logging functionlog activity (will also append additional names that identify the connection)
  * @arg {Cache} cache the cache that will handle the logevity of the SQL statement
  * @arg {function} logging the function(dbNames) that will return a name/dialect specific function(obj1OrMsg [, obj2OrSubst1, ..., obj2OrSubstN])) that will handle database logging
  * @arg {function} [readyCb] the callback function(error) when all of the connections/pools are ready for use
  */
  constructor(conf, cache, logging, readyCb) {
    const db = internal(this), connCnt = conf.db.connections.length;
    db.at.sqls = new Array(connCnt);
    db.at.log = (logging && logging(['db'])) || console.log;
    var ccnt = 0, errored;
    for (let i = 0, conn, def, orm, dlct, track = {}; i < connCnt; ++i) {
      conn = conf.db.connections[i];
      if (!conn.id) throw new Error(`Connection at index ${i} must have and "id"`);
      def = conf.univ.db[conn.id]; // pull host/credentials from external conf resource
      if (!def) throw new Error(`Connection at index ${i} has invalid "id": ${conn.id}`);
      conn.sql.host = conn.sql.host || def.host;
      dlct = conn.sql.dialect.toLowerCase();
      conn.sql.logging = conn.sql.log === false ? false : logging && logging([...conn.sql.log, 'db', conn.name, dlct, conn.service, conn.id, `v${conn.version || 0}`]); // override ORM logging
      if (dlct === 'oracle') {
        orm = new OracleDB(def.username, def.password, conn.sql, conn.service, conn.sid, conf.privatePath, track, conn.sql.logging, conf.debug);
      } else {
        orm = new Sequelize(conn.service || conn.sid, def.username, def.password, conn.sql); // TODO : Remove Sequalize (not needed)
        if (!orm.init) orm.init = function sequelizeInit(opts, scb) {
          if (scb) setImmediate(scb);
        };
      }
      // prepared SQL functions from file(s) that reside under the defined name and dialect (or "default" when dialect is flagged accordingly)
      if (db.this[conn.name]) throw new Error(`DB connection ID ${conn.id} cannot have a duplicate name for ${conn.name}`);
      db.at.sqls[i] = new SQLS(conf.mainPath, cache, conn.preparedSql, (db.this[conn.name] = {}), new DBO(orm, conf, conn), conn, connCb);
    }
    function connCb(err) {
      if (++ccnt && ((err && !errored && (errored = true)) || (!errored && ccnt === conf.db.connections.length))) {
        db.at.log(err || `${ccnt} DB(s) are ready for use`);
        if (readyCb) readyCb(err);
      }
    }
  }

  /**
   * Closes all DB pools/connections/etc.
   */
  close() {
    const db = internal(this);
    for (let i = 0, l = db.at.sqls.length; i < l; ++i) db.at.sqls[i].close();
  }
}

/**
* Reads all the perpared SQL definition files for a specified name directory and adds a function to execute the SQL file contents
*/
class SQLS {

  /**
  * Reads all the prepared SQL definition files for a specified name directory and adds a function to execute the SQL file contents
  * @constructs SQLS
  * @arg {String} sqlBasePth the absolute path that SQL files will be included
  * @arg {Cache} cache the cache that will handle the logevity of the SQL statement
  * @arg {Object} copt the cache options that will be passed into the {@link Cache#method} function that registers the cache function
  * @arg {Object} [copt.cache] set the caching options that will be passed the cache.method (false/null/undefined will turn caching of prepared SQL 
  * files off and static SQL files will be used instead- production use)
  * @arg {Object} db the object where SQL retrieval methods will be stored (by file name parts separated by a period- except the file extension)
  * @arg {DBO} dbo the DBO object to use
  * @arg {Object} conn the connection configuration
  */
  constructor(sqlBasePth, cache, copt, db, dbo, conn, dboReadyCb) {
    if (!cache) throw new Error('Options required');
    if (typeof cache.method !== 'function') throw new Error('Options "cache method" must be a function(cacheFuncName, funcToCache, cacheOptions)');
    if (typeof cache.methods !== 'object') throw new Error('Options "cache methods" must be an object where cached functions will be accessed');
    if (!conn.name) throw new Error('Connection ' + conn.id + ' must have a name');

    const bpth = Path.join(sqlBasePth, conn.dir || conn.name);
    const sqls = internal(this);

    sqls.at.cache = cache;
    sqls.at.noCache = !copt || !copt.cache || !cache;
    sqls.at.copt = copt;
    sqls.at.db = db;
    sqls.at.dbo = dbo;
    sqls.at.conn = conn;
    if (!sqls.at.noCache && !sqls.at.copt.generateKey) sqls.at.copt.generateKey = function genCacheKey(json) {
      return JSON.stringify(json); // use the JSON as the actual unique key for cache
    };
    Fs.readdir(bpth, function sqlDir(err, files) {
      if (err) throw err;
      sqls.at.numOfPreparedStmts = files.length;
      for (let fi = 0, nm, ns, ext, jso; fi < sqls.at.numOfPreparedStmts; ++fi) {
        ns = files[fi].split('.');
        ext = ns.pop();
        nm = 'sql_' + sqls.at.conn.name + '_' + sqls.at.conn.sql.dialect + '_' + ns.join('_');
        for (var ni = 0, nl = ns.length, so = sqls.at.db; ni < nl; ++ni) {
          so[ns[ni]] = so[ns[ni]] || (ni < nl - 1 ? {} : sqls.this.prepared(nm, Path.join(bpth, files[fi]), ext));
          so = so[ns[ni]];
        }
      }
      sqls.at.dbo.init({ numOfPreparedStmts: sqls.at.numOfPreparedStmts }, dboReadyCb);
    });
  }

  /**
   * Generates a function that will execute a pre-defined SQL statement contained within a SQL file
   * @arg {String} name the name of the SQL (excluding the extension)
   * @arg {String} fpth the path to the SQL file to execute
   * @arg {String} ext the file extension that will be used
   */
  prepared(name, fpth, ext) {
    const sqls = internal(this);
    // cache the SQL statement capture in order to accommodate dynamic file updates on expiration
    if (sqls.at.noCache) {
      sqls.at.cache = sqls.at.cache || { methods: {} };
      sqls.at.cache.methods[name] = {};
      if (sqls.at.conn.sql.logging) sqls.at.conn.sql.logging(`Setting static ${fpth} at ${JSON.stringify(sqls.at.copt)}`);
      readSqlFile(function readStaticFile(err, data) {
        sqls.at.cache.methods[name][ext] = function staticSql(opts, cb) {
          cb(err, data); // options are irrelevant
        };
      });
    } else sqls.at.cache.method(name + '.' + ext, function readCachedFile(opts, cb) {
      if (sqls.at.conn.sql.logging) sqls.at.conn.sql.logging(`Refreshing cached ${fpth} at ${JSON.stringify(sqls.at.copt)}`);
      readSqlFile(cb);
    }, sqls.at.copt);

    function readSqlFile(cb) {
      Fs.readFile(fpth, { encoding: 'utf8' }, function readSqlFileCb(err, data) {
        var dt;
        try {
          dt = !err && ext === 'json' ? JSON.parse(data.toString('utf8').replace(/^\uFEFF/, '')) : data;
        } catch (e) {
          err = e;
        }
        cb(err, dt || data);
      });
    }

    /**
    * Sets/formats SQL parameters and executes an SQL statement
    * @arg {Object} params the parameter names/values to pass into the query
    * @arg {String} [locale] the locale that will be used for date formatting
    * @arg {Object} [frags] the SQL fragments being used (if any)
    * @arg {function} cb the callback function(error, results)
    */
    return function execSqlPublic(params, locale, frags, cb) {
      params = params || {};
      var mopt = { params: params, opts: frags };
      if (sqls.at.conn.params) for (var i in sqls.at.conn.params) {
        if (typeof params[i] === 'undefined') params[i] = sqls.at.conn.params[i]; // add per connection static parameters when not overridden
      }
      if (params && locale) for (var i in params) params[i] = (params[i] instanceof Date && params[i].toISOString()) || params[i]; // convert dates to ANSI format for use in SQL
      sqls.at.cache.methods[name][ext](mopt, sqls.this.execCb(fpth, params, frags, {}, cb));
    };
  }

  execCb(fpth, params, frags, jopt, cb) {
    const sqls = internal(this);
    return function readSqlFileCb(err, sql) {
      if (err) return cb(err, sql);
      var qopt = { query: jopt.query || {}, replacements: params };
      sqls.at.dbo.exec(fpth, sql, qopt, frags, cb);
    };
  }

  close() {
    return internal(this).at.dbo.close();
  }

  get numOfPreparedStmts() {
    return internal(this).at.numOfPreparedStmts || 0;
  }
}

/**
* Database ORM
*/
class DBO {

  /**
  * Database ORM constructor
  * @constructs ORM
  * @arg {Object} orm the ORM to use
  * @arg {Object} conf the application configuration profile
  * @arg {Object} [conn] the connection configuration
  */
  constructor(orm, conf, conn) {
    const dbo = internal(this);
    dbo.at.orm = orm;
    dbo.at.conf = conf;
    dbo.at.conn = conn;
    dbo.at.logging = conn.sql.logging;
    dbo.at.dialect = conn.sql.dialect.toLowerCase();
    dbo.at.version = conn.version || 0;
  }

  /**
   * Initializes DBO
   * @arg {Object} [opts] initializing options passed into the underlying ORM
   */
  init(opts, cb) {
    const dbo = internal(this);
    dbo.at.orm.init(opts, cb);
  }

  /**
  * Executes SQL using the underlying framework API
  * @arg {String} fpth the originating file path where the SQL resides
  * @arg {String} sql the SQL to execute with optional fragment definitions {@link DBO#frag}
  * @arg {Object} opts the options passed to the SQL API
  * @arg {String[]} frags the frament keys within the SQL that will be retained
  * @arg {function} cb the callback function(error, results)
  */
  exec(fpth, sql, opts, frags, cb) {
    const dbo = internal(this);
    const sqlf = dbo.this.frag(sql, frags, opts.replacements);
    opts.type = opts.type || (dbo.at.orm.QueryTypes && dbo.at.orm.QueryTypes.SELECT) || 'SELECT';
    // framework that executes SQL may output SQL, so, we dont want to output it again if logging is on
    if (dbo.at.conf.debug && !dbo.at.logging && (dbo.at.conf.debug === true || (dbo.at.conf.debug.sql && dbo.at.conf.debug.sql.indexOf('request') >= 0))) {
      console.log('%s=============>>> %s%s', '\x1b[34m', `SQL ${fpth}`, '\x1b[0m');
      console.dir(opts, { colors: true });
      console.log('%s%s%s', '\x1b[36m', sqlf, '\x1b[0m');
      console.log('%s=============>>> %s%s', '\x1b[34m', 'SQL', '\x1b[0m');
    } else if (dbo.at.logging) {
      dbo.at.logging(`Executing SQL ${fpth}${opts && opts.replacements ? ` with replacements ${JSON.stringify(opts.replacements)}` : ''}${frags ? ` framents used ${JSON.stringify(frags)}` : ''}`);
    }
    dbo.at.orm[opts.type.toUpperCase() === 'SELECT' ? 'query' : opts.type.toLowerCase()](sqlf, opts).then(function thn(rslt) {
      if (dbo.at.conf.debug && !dbo.at.logging && (dbo.at.conf.debug === true || (dbo.at.conf.debug.sql && dbo.at.conf.debug.sql.indexOf('response') >= 0))) {
        console.log('%s=============<<< %s%s', '\x1b[32m', `SQL ${fpth} (connections: ${dbo.at.orm.lastConnectionCount || 'N/A'}, in use: ${dbo.at.orm.lastConnectionInUseCount || 'N/A'})`, '\x1b[0m');
        console.dir(rslt, { colors: true });
        console.log('%s=============<<< %s%s', '\x1b[32m', 'SQL', '\x1b[0m');
      } else if (dbo.at.logging) {
        dbo.at.logging(`SQL ${fpth} returned with ${(rslt && rslt.length) || 0} records (connections: ${dbo.at.orm.lastConnectionCount || 'N/A'}, in use: ${dbo.at.orm.lastConnectionInUseCount || 'N/A'})`);
      }
      cb(null, rslt);
    }).catch(function ctch(err, meta) {
      if (dbo.at.conf.debug && !dbo.at.logging && (dbo.at.conf.debug === true || (dbo.at.conf.debug.sql && dbo.at.conf.debug.sql.indexOf('error') >= 0))) {
        console.log('%s============<<< %s%s', '\x1b[41m\x1b[33m', `SQL ${fpth}  (connections: ${dbo.at.orm.lastConnectionCount || 'N/A'}, in use: ${dbo.at.orm.lastConnectionInUseCount || 'N/A'})`, '');
        console.dir(err, { colors: true });
        if (meta) console.dir(meta, { colors: true });
        console.log('%s============<<< %s%s', '\x1b[41m\x1b[33m', 'SQL', '\x1b[0m');
      } else if (dbo.at.logging) {
        dbo.at.logging(`SQL ${fpth} failed ${err.message || JSON.stringify(err)} (connections: ${dbo.at.orm.lastConnectionCount || 'N/A'}, in use: ${dbo.at.orm.lastConnectionInUseCount || 'N/A'})`);
      }
      cb(err);
    });
  }

  /**
  * Removes any SQL fragments that are wrapped around [[? someKey]] and [[?]] when the specified keys does not contain the discovered key (same for dialect and version keys)
  * Replaces any SQL parameters that are wrapped around :someParam with the indexed parameter names (i.e. :someParam :someParam1 ...) and adds the replacement value to the supplied replacements
  * @arg {String} sql the SQL to defragement
  * @arg {String[]} [keys] fragment keys which will remain intact within the SQL
  * @arg {Object} [rplmts] an object that contains the SQL parameterized replacements that will be used for parameterized array composition
  * @returns {String} the defragmented SQL
  */
  frag(sql, keys, rplmts) {
    if (!sql) return sql;
    const dbo = internal(this);

    sql = sql.replace(/(:)([a-z]+[0-9]*?)/gi, function sqlArrayRpl(match, pkey, key) {
      for (var i = 0, vals = key && rplmts && Array.isArray(rplmts[key]) && rplmts[key], keys = '', l = vals && vals.length; i < l; ++i) {
        keys += ((keys && ', ') || '') + pkey + key + (i || '');
        rplmts[key + (i || '')] = vals[i];
      }
      return keys || (pkey + key);
    });
    sql = sql.replace(/((?:\r?\n|\n)*)-{0,2}\[\[\!(?!\[\[\!)\s*(\w+)\s*\]\](?:\r?\n|\n)*([\S\s]*?)-{0,2}\[\[\!\]\]((?:\r?\n|\n)*)/g, function sqlDiaRpl(match, lb1, key, fsql, lb2) {
      return (key && key.toLowerCase() === dbo.at.dialect && fsql && (lb1 + fsql)) || ((lb1 || lb2) && ' ') || '';
    });
    sql = sql.replace(/((?:\r?\n|\n)*)-{0,2}\[\[version(?!\[\[version)\s*(=|<=?|>=?|<>)\s*[+-]?(\d+\.?\d*)\s*\]\](?:\r?\n|\n)*([\S\s]*?)-{0,2}\[\[version\]\]((?:\r?\n|\n)*)/gi, function sqlVerRpl(match, lb1, key, ver, fsql, lb2) {
      return (key && ver && !isNaN(ver = parseFloat(ver)) && compare[key](dbo.at.version, ver) && fsql && (lb1 + fsql)) || ((lb1 || lb2) && ' ') || '';
    });
    return sql.replace(/((?:\r?\n|\n)*)-{0,2}\[\[\?(?!\[\[\?)\s*(\w+)\s*\]\](?:\r?\n|\n)*([\S\s]*?)-{0,2}\[\[\?\]\]((?:\r?\n|\n)*)/g, function sqlFragRpl(match, lb1, key, fsql, lb2) {
      return (key && keys && keys.indexOf(key) >= 0 && fsql && (lb1 + fsql)) || ((lb1 || lb2) && ' ') || '';
    });
  }

  close() {
    return internal(this).at.orm.close();
  }
}

module.exports = DB;

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
