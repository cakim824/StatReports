const { sendPreparedStatementToPortalDB } = require('../../../utils/mariadb');

// var logger = require('../../../utils/logger')({
//   dirname: '',
//   filename: '',
//   sourcename: 'consults-history.js'
// });

const DATE_UNITS = {
    hourly: {
        column_name: "A.START_DT_HOUR"
    },
    daily: {
        column_name: "A.START_DT_DAY"
    },
    monthly: {
        column_name: "A.START_DT_MONTH"
    }
};

const getColumnName = date_unit => DATE_UNITS[date_unit].column_name;

const getConsultsHistory = async ({ date_unit, site_cd, start_date, end_date, start_time, end_time, agent_group, agent_id, empno_list, media_type, interaction_type, main_category, medium_category, sub_category }) => {

    const column_name = getColumnName(date_unit);

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

    var agent_group_query = "";
    var agent_id_query = "";
    var media_type_query = "";
    var interaction_type_query = "";
    var main_category_query = "";
    var medium_category_query = "";
    var sub_category_query = "";
    var time_range_query = "";

    if ( agent_group != "" ) {
        agent_group_query = ` AND EMP_NO IN ${empno_list}`;
    }
    if ( agent_id != "" ) {
        agent_id_query = ` AND EMP_NO = '${agent_id}'`;
    }
    if ( media_type != "" ) {
        media_type_query = ` AND CHANNEL = '${media_type}'`;
    }
    if ( interaction_type != "" ) {
        interaction_type_query = ` AND IO_GUBUN = '${interaction_type}'`;
    }
    if ( main_category != "" ) {
        main_category_query = ` AND CD01 = '${main_category}'`;
    }
    if ( medium_category != "" ) {
        medium_category_query = ` AND CD02 = '${medium_category}'`;
    }
    if ( sub_category != "" ) {
        sub_category_query = ` AND CD03 = '${sub_category}'`;
    }
    if ( start_time != "" ) {
        time_range_query = ` AND CONCAT(RIGHT(A.START_DT_HOUR, 2), '00') BETWEEN '${start_time}' AND '${end_time}'`;
    }
    //
    // var parameters = { date_unit, site_cd, start_date, end_date, in_type, in_detail_type, dnis_index, endpoint_index };
  
    var query =
    `
    SELECT 
	
		A.SITE_CD, 
        ${column_name},
		LEFT(${column_name}, 10) AS DATE_KEY,
		CONCAT(RIGHT(${column_name}, 2), ':00') AS TIME_KEY,
		(SELECT COM_ENM FROM tb_scode WHERE COM_LCD = 'CH001' AND COM_SCD = A.CHANNEL) AS CHANNEL, 
        (SELECT COM_ENM FROM tb_scode WHERE COM_LCD = 'IO000' AND COM_SCD = A.IO_GUBUN) AS INTERACTION,	
        (SELECT CD_NAME FROM tb_consult_code WHERE SITE_CD='${site_cd}' AND CD01=A.CD01 AND CD02='0000') AS CD01,	
        (SELECT CD_NAME FROM tb_consult_code WHERE SITE_CD='${site_cd}' AND CD01=A.CD01 AND CD02=A.CD02 AND CD02!='0000' AND CD03='0000') AS CD02,	
        (SELECT CD_NAME FROM tb_consult_code WHERE SITE_CD='${site_cd}' AND CD01=A.CD01 AND CD02=A.CD02 AND CD03=A.CD03 AND CD03!='0000') AS CD03,
        COUNT(*) AS COUNT,
        SEC_TO_TIME(CEILING(SUM(A.ENGAGE_TIME))) AS ENGAGE_TIME,
		SEC_TO_TIME(CEILING(CEILING(SUM(A.ENGAGE_TIME))/COUNT(*))) AS AVG_ENGAGE_TIME

    FROM (

	    SELECT
		 	SITE_CD, INDEX_NO, EMP_NO,
		 	LEFT(CONCAT(DATE_FORMAT(START_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(START_TIME, '%T')),7) AS START_DT_MONTH,
		 	LEFT(CONCAT(DATE_FORMAT(START_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(START_TIME, '%T')),10) AS START_DT_DAY,
		 	LEFT(CONCAT(DATE_FORMAT(START_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(START_TIME, '%T')),13) AS START_DT_HOUR,
		 	CONCAT(DATE_FORMAT(START_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(START_TIME, '%T')) AS START_DT,
            CONCAT(DATE_FORMAT(END_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(END_TIME, '%T')) AS END_DT,
            IF(CONCAT(START_DATE, START_TIME) > CONCAT(END_DATE, END_TIME), 0, TIME_TO_SEC(TIMEDIFF(CONCAT(DATE_FORMAT(END_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(END_TIME, '%T')), CONCAT(DATE_FORMAT(START_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(START_TIME, '%T'))))) AS ENGAGE_TIME,
            -- TIME_TO_SEC(TIMEDIFF(CONCAT(DATE_FORMAT(END_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(END_TIME, '%T')), CONCAT(DATE_FORMAT(START_DATE, '%Y-%m-%d'), ' ', TIME_FORMAT(START_TIME, '%T')))) AS ENGAGE_TIME,
			START_DATE, START_TIME, END_DATE, END_TIME,		 
            CUSTDATA_SEQ, CALL_NUMBER, CHANNEL, IO_GUBUN, 
            CD01,
            IF((CD02='선택하세요' OR CD02='CHOOSE'), '0000', IFNULL(NULLIF(CD02, ''), '0000')) AS CD02,	
            IF((CD03='선택하세요' OR CD03='CHOOSE'), '0000', IFNULL(NULLIF(CD03, ''), '0000')) AS CD03,
            CRT_DATE, CRT_EMP_NO, CALL_ID, CALL_UUID
	    FROM tb_consult
	    WHERE START_TIME != '' AND END_TIME != '' 
			AND SITE_CD = '${site_cd}'
            AND START_DATE BETWEEN '${start_date}' AND '${end_date}'
            ${agent_group_query}
            ${agent_id_query}
            ${media_type_query}
            ${interaction_type_query}
            ${main_category_query}
            ${medium_category_query}
            ${sub_category_query}
    ) A

    WHERE 1=1
        ${time_range_query}

    GROUP BY 
        ${column_name},
		A.CHANNEL, A.IO_GUBUN, A.CD01, A.CD02, A.CD03
    ;`
    ;
    
    const params = [ ];
    const rows = await sendPreparedStatementToPortalDB({ query });
    console.log("[consults-history][get]: query = " + query);

    return rows;
    
};


module.exports = {
    getConsultsHistory
};