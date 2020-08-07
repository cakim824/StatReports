var mariaDB = require('mysql');
var mssql = require('mssql');
var configMariaDB = require('./db_info').mariaDB;
const configMssql = require('./db_info').mssql;

module.exports.mariaDB = function () {
  return {
    init: function () {
      return mariaDB.createConnection({
        host: configMariaDB.host,
        port: configMariaDB.port,
        user: configMariaDB.user,
        password: configMariaDB.password,
        database: configMariaDB.database
      })
    },
    test_open: function (con) {
      con.connect(function (err) {
        if (err) {
          console.error('mariaDB connection error :' + err);
        } else {
          console.info('mariaDB is connected successfully.');
        }
      })
    }
  }
}();

module.exports.mssql = function () {
  return {
    init: function () {
      return new mssql.ConnectionPool(configMssql);
    },
    test_open: function (con) {
      con.connect().then(pool => {
        return pool.request().query("select 1");
      }).then(result => {
        console.log("mssql is connected succesfully.");
        mssql.close();
      }).catch(err => {
        console.log("mssql is connected error.");
        mssql.close();
      });
    }
  }
}();

