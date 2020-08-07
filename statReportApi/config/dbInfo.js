require( 'dotenv' ).config()

var mariaDB = require( './config' ).get( process.env.NODE_ENV ).mariaDB;
var mariaDB_APP = require( './config' ).get( process.env.NODE_ENV ).mariaDB_APP;
var mssql = require( './config' ).get( process.env.NODE_ENV ).mssql;

//MariaDB
module.exports.mariaDB = mariaDB;
module.exports.mariaDB_APP = mariaDB_APP;

//MS-sql
module.exports.mssql = mssql;

