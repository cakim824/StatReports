require('dotenv').config();

var express = require('express');
var bodyParser = require('body-parser');
var cors = require('cors');
var fs = require('fs');
var https = require('https');
var config = require('./config/config');

var app             = express();

var PORT = process.env.PORT || 9001;
process.setMaxListeners(0); // 하나의 라우트에서 생성가능한 최대 PATH 수 제한 해제


var mode = process.env.NODE_ENV, options;
if ( process.env.NODE_ENV === 'production' || process.env.NODE_ENV === 'development'  ) {
    options = {
        key: fs.readFileSync(`${config.get( mode ).ssl.baseDirPath}/${config.get( mode ).ssl.keyFileName}`),
        cert: fs.readFileSync(`${config.get( mode ).ssl.baseDirPath}/${config.get( mode ).ssl.certFileName}`),
        ca: fs.readFileSync(`${config.get( mode ).ssl.baseDirPath}/${config.get( mode ).ssl.caFileName}`)
    };
}

app.disable('x-powered-by');

app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());

var corsOptions = {
	"origin": "*",
	"methods": "GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS",
	"allowedHeaders": "*",
	"maxAge": 172000,
	"optionsSuccessStatus": 204
};
app.use(cors(corsOptions));



app.use('/api', require('./routes/api'));

app.use(function(err, req, res, next) {
    console.error(err.stack);
    res.status(500).send('문제가 있습니다.');
});

if ( process.env.PROTOCOL === 'http' ) {
    app.listen(PORT, () => {
        console.log("API SERVER LISTENING TO " + PORT);
     });
} else if ( process.env.PROTOCOL === 'https' ) {
    https.createServer(options, app).listen(PORT, () => {
        console.log(`reportApiServer listening on https://localhost:${PORT}`);
    });
}

module.exports = app;
