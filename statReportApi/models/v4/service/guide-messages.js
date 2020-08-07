const { sendPreparedStatementToPortalDB } = require('../../../utils/mariadb');
const { filterArgumentsNumber } = require('../../../utils/common');
// var logger = require('../../../utils/logger')({
//   dirname: '',
//   filename: '',
//   sourcename: 'guide-messages.js'
// });

var isNotEmpty = value => value != "";

const DATE_UNITS = {
    hourly: {
        date_length: "10",
        date_format: "%Y-%m-%d"
    },
    daily: {
        date_length: "8",
        date_format: "%Y-%m-%d"
    },
    monthly: {
        date_length: "6",
        date_format: "%Y-%m"
    }
};

const getDateLength = date_unit => DATE_UNITS[date_unit].date_length;
const getDateFormat = date_unit => DATE_UNITS[date_unit].date_format;

const getMsgRequestData = async ({ date_unit, site_cd, start_date, end_date, dnis, start_time, end_time }) => {

    const date_length = getDateLength(date_unit);
    const date_format = getDateFormat(date_unit);

    var madatoryParamList = ['site_cd', 'start_date', 'end_date'];
    var missingParamList = [];
    var existMandatoryParam = true;
  
    if (!site_cd) {
        existMandatoryParam = false;
        missingParamList.push('site_cd');
    }
    if (!start_date) {
        existMandatoryParam = false;
        missingParamList.push('start_date');
    }
    if (!end_date) {
        existMandatoryParam = false;
        missingParamList.push('end_date');
    }
  
    if (!existMandatoryParam) {
        var resData = {
            success: false,
            message: `필수 파라미터[${madatoryParamList.join(',')}] 중  [ ${missingParamList.join(',')}] 가 누락되었습니다.`
        };
        res.status(400).send(resData);
        return;
    }

    var filterDnis = filterArgumentsNumber(dnis);
    var dnis_query = "";
    var time_range_query = "";

    if (isNotEmpty(dnis)) {
        dnis_query = ` AND  CALLBACK IN ('${dnis}', '${filterDnis}')`;
    }
    if (isNotEmpty(start_time)) {
        time_range_query = ` AND SUBSTRING(REQUEST_DATE, 9, 4) BETWEEN '${start_time}' AND '${end_time}'`;
    }
  
    var query =
    `
    SELECT 
		 LEFT(REQUEST_DATE, ${date_length}) AS REQ_DATE_KEY, 
         DATE_FORMAT(LEFT(REQUEST_DATE, 8), '${date_format}') AS DATE_KEY,
         CONCAT(SUBSTRING(REQUEST_DATE, 9, 2), ':00') AS TIME_KEY,
         (SELECT SERVICE_NM FROM tb_service WHERE SERVICE_CD = A.SERVICE_CD) AS SERVICE_NM,
		 SITE_CD, USER_ID, SERVICE_CD, CALLBACK, COUNT(*) AS COUNT
		 -- REQUEST_DATE, MSG_TYPE, EMP_NO

    FROM tb_msg_request A

    WHERE 1=1 
         -- AND SITE_CD = '${site_cd}'
         AND USER_ID = (SELECT SMS_USER_ID FROM tb_site WHERE SITE_CD = '${site_cd}' AND SMS_USE_YN = 'Y')
         AND SERVICE_CD = (SELECT SERVICE_CD FROM tb_service WHERE SERVICE_NM LIKE '%GUIDESMS%')
		 AND REQUEST_DATE BETWEEN '${start_date}' AND '${end_date}'
         ${time_range_query}
         ${dnis_query}	

    GROUP BY  LEFT(REQUEST_DATE, ${date_length}), SERVICE_CD, CALLBACK
    ;
    `;
    
    const params = [ date_unit, site_cd, start_date, end_date, dnis, start_time, end_time ];
    const rows = await sendPreparedStatementToPortalDB({ query, params });
    console.log("[guide-messages.controller][get]: query = " + query);

    return rows;
    
};


module.exports = {
    getMsgRequestData
};