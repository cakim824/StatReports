var { sendPreparedStatementToInfomart } = require("../../../utils/mssql");
// var logger = require("../../../utils/logger")({
//   dirname: "",
//   filename: "",
//   sourcename: "agent-outputs.js"
// });

var isNotEmpty = value => value != "";

const DATE_UNITS = {
    hourly: {
        date_type: "CHAR(16)",
        main_view_name: "AG2_I_SESS_STATE_HOUR",
        sub_view_name: "AG2_AGENT_HOUR"
    },
    daily: {
        date_type: "CHAR(10)",
        main_view_name: "AG2_I_SESS_STATE_DAY",
        sub_view_name: "AG2_AGENT_DAY"
    },
    monthly: {
        date_type: "CHAR(7)",
        main_view_name: "AG2_I_SESS_STATE_MONTH",
        sub_view_name: "AG2_AGENT_MONTH"
    }
};

const getViewName = date_unit => DATE_UNITS[date_unit].main_view_name;
const getSubViewName = date_unit => DATE_UNITS[date_unit].sub_view_name;
const getDateType = date_unit => DATE_UNITS[date_unit].date_type;

var parameterTypes = {
  start_date: "VarChar",
  end_date: "VarChar",
  sms_start_date: "VarChar",
  sms_end_date: "VarChar",
  sms_tenant_key: "VarChar",
  sms_site_cd: "VarChar",
  tenant_key: "VarChar",
  site_cd: "VarChar",
  agent_group: "VarChar",
  agent_id: "VarChar",
  media_type: "VarChar",
  interaction_type: "Int",
  service_type: "VarChar",
  third_party_media: "VarChar"
};

const getAgentStatesInfos = async ({ date_unit, tenant_key, site_cd, agent_group, agent_id, start_date, end_date, start_time, end_time }) => {

    const main_view_name = getViewName(date_unit);
    const sub_view_name = getSubViewName(date_unit);
    const date_type = getDateType(date_unit);

    var agent_group_query = "";
    var agent_id_query = "";
    var time_range_query = "";

    if (isNotEmpty(agent_group)) {
        agent_group_query = ` AND B.GROUP_KEY = ${agent_group}`;
    }
    if (isNotEmpty(agent_id)) {
        agent_id_query = ` AND A.RESOURCE_KEY = ${agent_id}`;
    }
    if ( start_time != "" ) {
        time_range_query = `  AND Y.TIME_KEY BETWEEN '${start_time}' AND '${end_time}'`;
    }

    var parameters = {
        tenant_key,
        site_cd,
        agent_group,
        agent_id,
        start_date,
        end_date,   
    };
  
    const query = `
    SET ANSI_WARNINGS OFF
    SET ARITHIGNORE ON
    SET ARITHABORT OFF
    
    SELECT *
    FROM (
    SELECT 
            ISNULL(X1.DT_KEY, X2.DT_KEY) AS DT_KEY,
            LEFT(ISNULL(X1.DT_KEY, X2.DT_KEY), 10) as DATE_KEY,
			RIGHT(ISNULL(X1.DT_KEY, X2.DT_KEY), 5) as TIME_KEY,
            ISNULL(X1.AGENT_NAME, X2.AGENT_NAME) AS AGENT_NAME,
            X1.M_TYPE, X1.ACTIVE_TIME, X1.READY_TIME, X1.BUSY_TIME, X1.NOT_READY_TIME, X1.WRAP_TIME,
    
            ISNULL(X2.IB_OFFERED, 0) AS IB_OFFERED,
            ISNULL(X2.IB_ENGAGE, 0) AS IB_ENGAGE,
            ISNULL(X2.IB_ENGAGE_TIME, 0) AS IB_ENGAGE_TIME,
            ISNULL(X2.OB_ENGAGE, 0) AS OB_ENGAGE,
            ISNULL(X2.OB_ENGAGE_TIME, 0) AS OB_ENGAGE_TIME,
            ISNULL(X2.INVITE_TIME, 0) AS INVITE_TIME,
            -- X2.IB_ENGAGE, X2.IB_ENGAGE_TIME, X2.OB_ENGAGE, X2.OB_ENGAGE_TIME
    
            -- X1.BUSY_TIME, 
            -- X1.WRAP_TIME,
            CEILING(ISNULL(CONVERT(FLOAT,INVITE_TIME)/IB_OFFERED, 0)) AS AVG_INVITE_TIME,
            CEILING(ISNULL(CONVERT(FLOAT,X1.BUSY_TIME)/(IB_ENGAGE + OB_ENGAGE), 0)) AS AVG_BUSY_TIME,
            CEILING(ISNULL(CONVERT(FLOAT,X1.WRAP_TIME)/(IB_ENGAGE + OB_ENGAGE), 0)) AS AVG_WRAP_TIME,
            CEILING(ISNULL(CONVERT(FLOAT,IB_ENGAGE_TIME)/IB_ENGAGE, 0)) AS AVG_IB_ENGAGE_TIME,
            CEILING(ISNULL(CONVERT(FLOAT,OB_ENGAGE_TIME)/OB_ENGAGE, 0)) AS AVG_OB_ENGAGE_TIME
    
    FROM 
    (
        SELECT 
            -- ROW_NUMBER() OVER(ORDER BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY) AS NUM
            (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
              FROM   DATE_TIME DT
              WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY) as DT_KEY
           , B.AGENT_NAME
           , (SELECT M.MEDIA_NAME_CODE
              FROM MEDIA_TYPE M
              WHERE M.MEDIA_TYPE_KEY = A.MEDIA_TYPE_KEY
             ) as M_TYPE
           , A.ACTIVE_TIME -- Session 유지시간
           , A.READY_TIME  -- 대기시간
           , A.BUSY_TIME   -- 처리소요시간
           , A.NOT_READY_TIME -- 이석시간
           , A.WRAP_TIME    -- 후처리 시간
        FROM  ${main_view_name} A
            , ( SELECT DISTINCT A.RESOURCE_KEY, C.AGENT_FIRST_NAME as AGENT_NAME, C.EMPLOYEE_ID 
                FROM RESOURCE_GROUP_FACT_ A, GROUP_ B, RESOURCE_ C, TENANT T
                WHERE A.GROUP_KEY = B.GROUP_KEY
                AND   A.TENANT_KEY = T.TENANT_KEY
                AND   B.TENANT_KEY = T.TENANT_KEY
                AND   C.TENANT_KEY = T.TENANT_KEY
                AND   A.RESOURCE_KEY = C.RESOURCE_KEY
                AND ((A.START_DATE_TIME_KEY <= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${start_date}')))
                      OR
                    (A.END_DATE_TIME_KEY >= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${end_date}'))))
                AND T.TENANT_KEY = ${tenant_key}
                AND C.AGENT_LAST_NAME = '${site_cd}'
                ${agent_group_query}
                ${agent_id_query}
              ) B
      WHERE A.RESOURCE_KEY = B.RESOURCE_KEY
       AND  (A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE=CONVERT(datetime, '${start_date}'))
                                AND (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE=CONVERT(datetime, '${end_date}')))
       AND A.MEDIA_TYPE_KEY = 1
    
      -- ORDER BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY
    ) X1
    
    FULL OUTER JOIN 
    
    (
        SELECT 
                -- ROW_NUMBER() OVER(ORDER BY AGENT_NAME, DT_KEY) AS NUM,
                ISNULL(T1.DT_KEY, T2.DT_KEY) AS DT_KEY,
                ISNULL(T1.AGENT_NAME, T2.AGENT_NAME) AS AGENT_NAME,
    
                T1.INVITE_TIME, T1.IB_OFFERED, T1.IB_ENGAGE, T1.IB_ENGAGE_TIME, T2.OB_ENGAGE, T2.OB_ENGAGE_TIME
    
                -- ISNULL(T1.IB_ENGAGE, 0) AS IB_ENGAGE,
                -- ISNULL(T1.IB_ENGAGE_TIME, 0) AS IB_ENGAGE_TIME,
                -- ISNULL(T2.OB_ENGAGE, 0) AS OB_ENGAGE,
                -- ISNULL(T2.OB_ENGAGE_TIME, 0) AS OB_ENGAGE_TIME
    
              -- ROW_NUMBER() OVER(ORDER BY A.AGENT_NAME, A.DT_KEY, A.M_TYPE, A.I_TYPE) AS NUM
              -- , A.DT_KEY , A.AGENT_NAME , A.M_TYPE , A.I_TYPE , A.OFFERED , A.FAILED , A.ENGAGE , A.ENGAGE_TIME , A.TRANSFER_INIT_AGENT , A.XFER_RECEIVED_ACCEPTED
          FROM (SELECT (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
                        FROM   DATE_TIME DT
                        WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY
                        ) as DT_KEY, 
                       B.AGENT_NAME AS AGENT_NAME,
                       SUM(A.INVITE_TIME) AS INVITE_TIME,
                       SUM(A.OFFERED) as IB_OFFERED, 
                       SUM(A.ENGAGE) as IB_ENGAGE,
                       SUM(A.ENGAGE_TIME) as IB_ENGAGE_TIME
               FROM ${sub_view_name} A INNER JOIN
                       (SELECT DISTINCT A.RESOURCE_KEY, C.AGENT_FIRST_NAME as AGENT_NAME
                        FROM RESOURCE_GROUP_FACT_ A
                             INNER JOIN GROUP_ B ON (A.TENANT_KEY = B.TENANT_KEY AND A.GROUP_KEY = B.GROUP_KEY)
                             INNER JOIN RESOURCE_ C ON (A.TENANT_KEY = C.TENANT_KEY AND A.RESOURCE_KEY = C.RESOURCE_KEY)
                             INNER JOIN TENANT T ON (A.TENANT_KEY = T.TENANT_KEY)
                        WHERE B.GROUP_TYPE ='Agent'
                          AND ((A.START_DATE_TIME_KEY <= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${start_date}')))
                               OR
                               (A.END_DATE_TIME_KEY >= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${end_date}'))))
                          AND   A.ACTIVE_FLAG = 1
                          AND   T.TENANT_KEY = ${tenant_key}
                          AND   C.AGENT_LAST_NAME = '${site_cd}'
                          ${agent_group_query}
                          ${agent_id_query}
                           
                       ) B ON (A.RESOURCE_KEY = B.RESOURCE_KEY)
               WHERE  A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${start_date}'))
                      AND (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${end_date}'))
                      AND A.MEDIA_TYPE_KEY = 1
                      AND A.INTERACTION_TYPE_KEY IN (2, 8, 9, 10, 11, 19, 20, 21)
                
               GROUP BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY, A.INTERACTION_TYPE_KEY
               ) T1
    
               FULL OUTER JOIN
    
               (SELECT (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
                        FROM   DATE_TIME DT
                        WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY
                        ) as DT_KEY, 
                        B.AGENT_NAME AS AGENT_NAME,
                        SUM(A.ENGAGE) as OB_ENGAGE, SUM(A.ENGAGE_TIME) as OB_ENGAGE_TIME
                FROM ${sub_view_name} A INNER JOIN
                        (SELECT DISTINCT A.RESOURCE_KEY, C.AGENT_FIRST_NAME as AGENT_NAME
                        FROM RESOURCE_GROUP_FACT_ A
                                INNER JOIN GROUP_ B ON (A.TENANT_KEY = B.TENANT_KEY AND A.GROUP_KEY = B.GROUP_KEY)
                                INNER JOIN RESOURCE_ C ON (A.TENANT_KEY = C.TENANT_KEY AND A.RESOURCE_KEY = C.RESOURCE_KEY)
                                INNER JOIN TENANT T ON (A.TENANT_KEY = T.TENANT_KEY)
                        WHERE B.GROUP_TYPE ='Agent'
                            AND ((A.START_DATE_TIME_KEY <= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${start_date}')))
                                 OR
                                (A.END_DATE_TIME_KEY >= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${end_date}'))))
                            AND   A.ACTIVE_FLAG = 1
                            AND   T.TENANT_KEY = ${tenant_key}
                            AND   C.AGENT_LAST_NAME = '${site_cd}'
                            ${agent_group_query}
                            ${agent_id_query}
                             
                        ) B ON (A.RESOURCE_KEY = B.RESOURCE_KEY)
                WHERE  A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${start_date}'))
                        AND (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${end_date}'))
                        AND A.MEDIA_TYPE_KEY = 1
                        AND A.INTERACTION_TYPE_KEY IN (3, 4, 5, 12, 13, 14, 15, 16, 17, 18, 22)
                 
                 GROUP BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY, A.INTERACTION_TYPE_KEY
                ) T2
    
                ON T1.DT_KEY = T2.DT_KEY AND T1.AGENT_NAME = T2.AGENT_NAME
    )X2
    
    ON X1.DT_KEY = X2.DT_KEY AND  X1.AGENT_NAME = X2.AGENT_NAME
    ) Y
    WHERE 1=1
    ${time_range_query}
    ;`
    ;
  
    console.log("[agent-states-infos] query: " + query + "\n    parameters: " + JSON.stringify(parameters));

    const rows = await sendPreparedStatementToInfomart(query, parameters)
    console.log("[agent-states-infos] result: " + JSON.stringify(rows));
    return rows;
    
};


const getAgentLoginInfos = async ({ tenant_key, site_cd, agent_group, agent_id, start_date, end_date }) => {

    var agent_group_query = "";
    var agent_id_query = "";

    if (isNotEmpty(agent_group)) {
        agent_group_query = ` AND B.GROUP_KEY = ${agent_group}`;
    }
    if (isNotEmpty(agent_id)) {
        agent_id_query = ` AND A.RESOURCE_KEY = ${agent_id}`;
    }

    var query =
    `
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
                            
          AND S.TENANT_KEY = ${tenant_key}     -- AND S.TENANT_KEY = 1
          AND  R.AGENT_LAST_NAME = '${site_cd}'    -- AND R.AGENT_LAST_NAME = 'TRIPLE'
          ${agent_group_query}  -- AND B.GROUP_KEY IN (399,390,398) 
          ${agent_id_query}     -- AND A.RESOURCE_KEY = '2307' 
      GROUP BY R. AGENT_FIRST_NAME, CONVERT(VARCHAR(10), (DATEADD(S, S.START_TS + 9*3600, '1970-01-01')), 120)
    ) A
  
    ORDER BY NUM ;
  
    `;
  
    const parameters = {tenant_key, site_cd, start_date, end_date};

    console.log("[agent-login-infos] query: " + query + "\n    parameters: " + parameters);
    const rows = await sendPreparedStatementToInfomart(query, parameters);
    console.log("[agent-login-infos] result: " + JSON.stringify(rows));
  
    return rows;
    
    
  };



module.exports = {
    getAgentStatesInfos,
    getAgentLoginInfos
}