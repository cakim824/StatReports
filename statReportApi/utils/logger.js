var winston = require('winston'), expressWinston = require('express-winston');
winston.transports.DailyRotateFile = require('winston-daily-rotate-file');
winston.emitErrs = false;

const fs = require('fs');
const logDir = 'logs';

if ( !fs.existsSync(logDir) ) {
    fs.mkdirSync(logDir);
}

function logTimeStamp() {
    var currentDate = new Date();

    var yyyy = currentDate.getFullYear().toString();
    var MM = (currentDate.getMonth() + 1).toString();
    var dd = currentDate.getDate().toString();

    var HH = currentDate.getHours().toString();
    var mm = currentDate.getMinutes().toString();
    var ss = currentDate.getSeconds().toString();

    return yyyy + '-' + (MM[1] ? MM : '0'+MM[0]) + '-' + (dd[1] ? dd : '0'+dd[0]) + ' ' + (HH[1] ? HH : '0'+HH[0]) +  ':' + (mm[1] ? mm : '0'+mm[0]) + ':' + (ss[1] ? ss : '0'+ss[0]);
};

var customLevels = {
    levels: {
        error: 0,
        warn: 1,
        info: 2,
        debug: 3
    },
    colors: {
        error: 'red',
        warn: 'yellow',
        info: 'green',
        debug: 'gray'
    }
};

winston.addColors(customLevels.colors);

var logger = function(options) {

    var sourcename = options.sourcename || '';

    return new (winston.Logger)({
        levels: customLevels.levels,
        colors: customLevels.colors,
        transports: [
            new winston.transports.Console({
                level: 'debug',
                handleExceptions: true,
                json: false,
                colorize: true
                // formatter: function(options) {
                //     return '[' + options.level + '][' + logTimeStamp() + '][' + sourcename + '] ' + options.message;
                // }
            }),
            new winston.transports.DailyRotateFile({
                level: process.env.LOG_LEVEL,
                dirname: options.dirname || process.env.LOG_DIRNAME,
                filename: options.filename || process.env.LOG_FILE_PREFIX, //'reportApiServer_',
                datePattern: 'yyyyMMdd.log',
                handleExceptions: false,
                json: false,
                maxsize: 5242880, //5MB
                maxdays: 15,
                // maxsize: 2, //5MB
                formatter: function(options) {
                    return '[' + options.level + '][' + logTimeStamp() + '][' + sourcename + '] ' + options.message;
                }
            })
        ],
        exceptionHandlers: [
            new winston.transports.DailyRotateFile({
                level: 'error',
                dirname: options.dirname || process.env.LOG_DIRNAME,
                filename: options.filename || process.env.LOG_FILE_PREFIX,
                datePattern: 'err_yyyyMMdd.log',
                handleExceptions: true,
                json: false,
                maxsize: 5242880, //5MB
                // maxsize: 2, //5MB
                formatter: function(options) {
                    return '[' + options.level + '][' + logTimeStamp() + '][' + sourcename + '] ' + options.message;
                }
            })
        ],
        exitOnError: false
    });
}

module.exports = logger;
