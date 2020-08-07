// var logger = require('../../../utils/logger')({ dirname: '', filename: '', sourcename: 'agent-login-infos.controller.js' });

const { sendPreparedStatementToPortalDB } = require('../../../utils/mariadb');
const { sendPreparedStatementToInfomart, sendPreparedQueryToInfomart } = require('../../../utils/mssql');

const getSummaryData = async ({ tenant_key, site_cd, agent_group, agent_id, start_date, end_date }) => {

  try{
    const tenant_key_query = `AND TENANT_KEY = ${tenant_key}`;
    const site_cd_query = `AND SITE_CD = '${site_cd}'`;

    var agent_group_query = '',
    agent_id_query = '';

    if (agent_group != '') {
      agent_group_query = `AND GROUP_KEY = ${agent_group}`;
    }
    if (agent_id != '') {
      agent_id_query = `AND RESOURCE_KEY = ${agent_id}`;
    }

    // const start_date_query = `AND START_TS >= ${start_date}`;
    // const end_date_query = `AND START_TS <= ${end_date}`;

    var query = `
    SELECT 	@ROWNUM := @ROWNUM + 1 AS NUM,
			A.DT_KEY,
			A.AGENT_NAME,
			A.LOGIN_TIME,
			A.LOGOUT_TIME
  
    FROM
    (
    SELECT
			LOGIN_DAY AS DT_KEY,
			EMP_NM AS AGENT_NAME,
			START_TS AS LOGIN_TIME,
			END_TS AS LOGOUT_TIME

    FROM tb_login_summary

    WHERE 1=1
    ${tenant_key_query}
    ${site_cd_query}
    ${agent_group_query}  
    ${agent_id_query}
    -- AND TENANT_KEY = '1'
		-- AND SITE_CD = 'DEV'
		-- AND GROUP_KEY = ''
		-- AND RESOURCE_KEY = ''
		AND START_TS >= '${start_date}'
		AND START_TS <= '${end_date}'

    GROUP BY EMP_NM, LOGIN_DAY
    ORDER BY EMP_NM, LOGIN_DAY
    ) A, (SELECT @rownum :=0) AS R
    ;
    `;

  console.log("[agent-login-infos] portal_query: " + query + "\n");
  const params = [];
  const rows = await sendPreparedStatementToPortalDB({ query, params });
  console.log("[agent-login-infos] rows: " + JSON.stringify(rows) + "\n");
  return rows;

  } catch(error) {
    throw error;
  }

};

const getInfomartData = async ({ tenant_key, site_cd, agent_group, agent_id, start_date, end_date }) => {

  try {
    const tenant_key_query = `AND S.TENANT_KEY = ${tenant_key}`;
    const site_cd_query = `AND R.AGENT_LAST_NAME = '${site_cd}'`;

    var agent_group_query = '',
      agent_id_query = '';

    if (agent_group != '') {
      agent_group_query = `AND B.GROUP_KEY = ${agent_group}`;
    }
    if (agent_id != '') {
      agent_id_query = `AND A.RESOURCE_KEY = ${agent_id}`;
    }
  
    const query = `
    SELECT 
	    ROW_NUMBER() OVER(ORDER BY A.AGENT_NAME, A.LOGIN_DAY) AS NUM,
      A.LOGIN_DAY AS DT_KEY,
      A.AGENT_NAME AS AGENT_NAME,
      CONVERT(VARCHAR(50), (DATEADD(S, A.START_TS + 9*3600, '1970-01-01')), 20) as LOGIN_TIME,
      (CASE WHEN ((CONVERT(VARCHAR(50), (DATEADD(S, A.END_TS + 9*3600, '1970-01-01')), 20)) > '${end_date}') THEN ' '
      ELSE (CONVERT(VARCHAR(50), (DATEADD(S, A.END_TS + 9*3600, '1970-01-01')), 20)) END) as LOGOUT_TIME 

    FROM
    (
    SELECT R.AGENT_FIRST_NAME AS AGENT_NAME,
           CONVERT(VARCHAR(10), (DATEADD(S, S.START_TS + 9*3600, '1970-01-01')), 120) as LOGIN_DAY,
           MIN(S.START_TS) AS START_TS, 
           MAX(S.END_TS) AS END_TS
    FROM SM_RES_SESSION_FACT S, RESOURCE_ R, RESOURCE_GROUP_FACT_ A, GROUP_ B 
    WHERE S.RESOURCE_KEY = R.RESOURCE_KEY
        AND A.GROUP_KEY = B.GROUP_KEY
        AND A.RESOURCE_KEY = R.RESOURCE_KEY
        AND  (S.START_DATE_TIME_KEY between (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE=CONVERT(datetime, '${start_date}'))
							AND	(SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE=CONVERT(datetime, '${end_date}')))
        ${tenant_key_query}   -- AND S.TENANT_KEY = 1
        ${site_cd_query}      -- AND R.AGENT_LAST_NAME = 'TRIPLE'
        ${agent_group_query}
        ${agent_id_query}
        
    GROUP BY R. AGENT_FIRST_NAME, CONVERT(VARCHAR(10), (DATEADD(S, S.START_TS + 9*3600, '1970-01-01')), 120)
    ) A

    ORDER BY NUM ;
    `;

    console.log("[agent-login-infos] infomart_query: " + query + "\n");
    const parameter_types = {};
    const parameters = {};
    const rows = await sendPreparedStatementToInfomart(query, parameters, parameter_types);
    console.log("[agent-login-infos] rows: " + JSON.stringify(rows) + "\n");
    return rows;

    } catch (error) {
      throw error;
    }

};

module.exports = {
  getSummaryData,
  getInfomartData
}
