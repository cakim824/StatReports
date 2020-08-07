var config = {
    local: {
        mariaDB: {
            host: '172.27.2.140' 
            ,port:3306
            ,user: 'agentDesktop_service'
            ,password: 'desk@dmin18'
            ,database: 'ccloud'
            ,multipleStatements :true
            ,charset:'utf8'
        },
        mariaDB_APP: {
            host: '172.27.2.140'
            ,port:3306
            ,user: 'agentDesktop_service'
            ,password: 'desk@dmin18'
            ,database: 'c_cloud'
            ,multipleStatements :true
            ,charset:'utf8'
        },
        mssql: {
            server: '172.27.2.130' 
            ,port: 1433
            ,user: 'dev_infomart'
            ,password: 'uDevinfomart@'
            ,database: 'InfoMart'
        }
    },
    development: {
        mariaDB: {
            host: '172.27.2.140' 
            ,port:3306
            ,user: 'agentDesktop_service'
            ,password: 'desk@dmin18'
            ,database: 'ccloud'
            ,multipleStatements :true
            ,charset:'utf8'
        },
        mariaDB_APP: {
            host: '172.27.2.140'
            ,port:3306
            ,user: 'agentDesktop_service'
            ,password: 'desk@dmin18'
            ,database: 'c_cloud'
            ,multipleStatements :true
            ,charset:'utf8'
        },
        mssql: {
            server: '172.27.2.130' 
            ,port: 1433
            ,user: 'dev_infomart'
            ,password: 'uDevinfomart@'
            ,database: 'InfoMart'
        },
        ssl: {
            baseDirPath: './ssl',
            keyFileName: 'private.key',
            certFileName: '',
            caFileName: '',
        }
    },
    production: {
        mariaDB: {
            host: '172.27.2.140' 
            ,port:3306
            ,user: 'agentDesktop_service'
            ,password: 'desk@dmin18'
            ,database: 'ccloud'
            ,multipleStatements :true
            ,charset:'utf8'
        },
        mariaDB_APP: {
            host: '172.27.2.140'
            ,port:3306
            ,user: 'agentDesktop_service'
            ,password: 'desk@dmin18'
            ,database: 'c_cloud'
            ,multipleStatements :true
            ,charset:'utf8'
        },
        mssql: {
            server: '172.27.2.130' 
            ,port: 1433
            ,user: 'dev_infomart'
            ,password: 'uDevinfomart@'
            ,database: 'InfoMart'
        },
        ssl: {
            baseDirPath: './ssl',
            keyFileName: 'private.key',
            certFileName: '',
            caFileName: '',
        }
    }
}

module.exports.get = function( env ) {
    return config[env] || config['default'];
}