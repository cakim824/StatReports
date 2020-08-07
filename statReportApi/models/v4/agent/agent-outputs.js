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
        main_view_name: "AG2_AGENT_HOUR"
    },
    daily: {
        date_type: "CHAR(10)",
        main_view_name: "AG2_AGENT_DAY"
    },
    monthly: {
        date_type: "CHAR(7)",
        main_view_name: "AG2_AGENT_MONTH"
    }
};

const getViewName = date_unit => DATE_UNITS[date_unit].main_view_name;
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

const getAgentOutputs = async ({ date_unit, tenant_key, site_cd, agent_group, agent_id, start_date, end_date, start_time, end_time, inbound_type_keys, outbound_type_keys, internal_type_keys }) => {

    const main_view_name = getViewName(date_unit);
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
        time_range_query = `  AND X.TIME_KEY BETWEEN '${start_time}' AND '${end_time}'`;
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
            ROW_NUMBER() OVER(ORDER BY ISNULL(ISNULL(T1.DT_KEY, T2.DT_KEY), T3.DT_KEY), ISNULL(ISNULL(T1.AGENT_NAME, T2.AGENT_NAME), T3.AGENT_NAME)) AS NUM
            , ISNULL(ISNULL(T1.DT_KEY, T2.DT_KEY), T3.DT_KEY) AS DT_KEY
            , LEFT(ISNULL(ISNULL(T1.DT_KEY, T2.DT_KEY), T3.DT_KEY), 10) AS DATE_KEY
            , RIGHT(ISNULL(ISNULL(T1.DT_KEY, T2.DT_KEY), T3.DT_KEY), 5) AS TIME_KEY
            , ISNULL(ISNULL(T1.AGENT_NAME, T2.AGENT_NAME), T3.AGENT_NAME) AS AGENT_NAME

            , ISNULL(IB_OFFERED, 0) AS IB_OFFERED	-- IB요청건수
            , ISNULL(IB_ENGAGE, 0) AS IB_ENGAGE
            , ISNULL(IB_ENGAGE_TIME, 0) AS IB_ENGAGE_TIME
            , ISNULL(CONVERT(NUMERIC(13,2), ROUND((ISNULL(IB_ENGAGE, 0) / ISNULL(IB_OFFERED, 0) * 100) ,2)), 0) AS IB_RESPONSE_RATE  -- IB응대율

            , ISNULL(OB_OFFERED, 0) AS OB_OFFERED	-- OB요청건수
            , ISNULL(OB_ENGAGE, 0) AS OB_ENGAGE
            , ISNULL(OB_ENGAGE_TIME, 0) AS OB_ENGAGE_TIME
            , ISNULL(CONVERT(NUMERIC(13,2), ROUND((ISNULL(OB_ENGAGE, 0) / ISNULL(OB_OFFERED, 0) * 100) ,2)), 0) AS OB_RESPONSE_RATE  -- OB응대율

            , ISNULL(IN_OFFERED, 0) AS IN_OFFERED	-- IN요청건수
            , ISNULL(IN_ENGAGE, 0) AS IN_ENGAGE
            , ISNULL(IN_ENGAGE_TIME, 0) AS IN_ENGAGE_TIME

            , (ISNULL(IB_TRANSFER_INIT_AGENT, 0) + ISNULL(OB_TRANSFER_INIT_AGENT, 0)) AS TRANSFER_INIT_AGENT	-- 호전환시도건수
            , (ISNULL(IB_XFER_RECEIVED_ACCEPTED, 0) + ISNULL(OB_XFER_RECEIVED_ACCEPTED, 0)) AS XFER_RECEIVED_ACCEPTED	-- 호전환수신건수

            -- ROW_NUMBER() OVER(ORDER BY A.AGENT_NAME, A.DT_KEY, A.M_TYPE, A.I_TYPE) AS NUM
            -- , A.DT_KEY , A.AGENT_NAME , A.M_TYPE , A.I_TYPE , A.OFFERED , A.FAILED , A.ENGAGE , A.ENGAGE_TIME , A.TRANSFER_INIT_AGENT , A.XFER_RECEIVED_ACCEPTED
            
	FROM (SELECT (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
                    FROM   DATE_TIME DT
                    WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY
                    ) as DT_KEY 
                   , B.AGENT_NAME AS AGENT_NAME
                   , SUM(A.OFFERED) as IB_OFFERED
                   , SUM(A.OFFERED) - SUM(A.ENGAGE) as IB_FAILED
                   , SUM(A.ENGAGE) as IB_ENGAGE
                   , SUM(A.ENGAGE_TIME) as IB_ENGAGE_TIME  
                   , SUM(A.TRANSFER_INIT_AGENT) as IB_TRANSFER_INIT_AGENT
                   , SUM(A.XFER_RECEIVED_ACCEPTED) as IB_XFER_RECEIVED_ACCEPTED 
           FROM ${main_view_name} A INNER JOIN
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
                   ) B ON (A.RESOURCE_KEY = B.RESOURCE_KEY)
           WHERE  A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${start_date}'))
                  AND (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${end_date}'))
                  AND A.MEDIA_TYPE_KEY = 1
                  AND A.INTERACTION_TYPE_KEY IN  ${inbound_type_keys}
           ${agent_id_query}
           GROUP BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY, A.INTERACTION_TYPE_KEY
           ) T1

           FULL OUTER JOIN

           (SELECT (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
                    FROM   DATE_TIME DT
                    WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY
                    ) as DT_KEY, 
                    B.AGENT_NAME AS AGENT_NAME,
                    SUM(A.OFFERED) as OB_OFFERED, SUM(A.OFFERED) - SUM(A.ENGAGE) as OB_FAILED, SUM(A.ENGAGE) as OB_ENGAGE, SUM(A.ENGAGE_TIME) as OB_ENGAGE_TIME 
                    , SUM(A.TRANSFER_INIT_AGENT) as OB_TRANSFER_INIT_AGENT, SUM(A.XFER_RECEIVED_ACCEPTED) as OB_XFER_RECEIVED_ACCEPTED
            FROM ${main_view_name} A INNER JOIN
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
                    ) B ON (A.RESOURCE_KEY = B.RESOURCE_KEY)
            WHERE  A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${start_date}'))
                    AND (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${end_date}'))
                    AND A.MEDIA_TYPE_KEY = 1
                    AND A.INTERACTION_TYPE_KEY IN ${outbound_type_keys}
            ${agent_id_query}
             GROUP BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY, A.INTERACTION_TYPE_KEY
            ) T2

            ON T1.DT_KEY = T2.DT_KEY AND T1.AGENT_NAME = T2.AGENT_NAME

            FULL OUTER JOIN

           (SELECT (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
                    FROM   DATE_TIME DT
                    WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY
                    ) as DT_KEY, 
                    B.AGENT_NAME AS AGENT_NAME,
                    SUM(A.OFFERED) as IN_OFFERED, SUM(A.OFFERED) - SUM(A.ENGAGE) as IN_FAILED, SUM(A.ENGAGE) as IN_ENGAGE, SUM(A.ENGAGE_TIME) as IN_ENGAGE_TIME 
                    , SUM(A.TRANSFER_INIT_AGENT) as IN_TRANSFER_INIT_AGENT, SUM(A.XFER_RECEIVED_ACCEPTED) as IN_XFER_RECEIVED_ACCEPTED
            FROM ${main_view_name} A INNER JOIN
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
                    ) B ON (A.RESOURCE_KEY = B.RESOURCE_KEY)
             WHERE  A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${start_date}'))
                    AND (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, '${end_date}'))
                    AND A.MEDIA_TYPE_KEY = 1
                    AND A.INTERACTION_TYPE_KEY IN ${internal_type_keys}
             ${agent_id_query}
             GROUP BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY, A.INTERACTION_TYPE_KEY
            ) T3

            ON (T1.DT_KEY = T3.DT_KEY AND T1.AGENT_NAME = T3.AGENT_NAME) 
                OR (T2.DT_KEY = T3.DT_KEY AND T2.AGENT_NAME = T3.AGENT_NAME)

    ) X
    WHERE 1=1
    ${time_range_query}
    ; `
    ;
  
    console.log("[agent-outputs] query: " + query + "\n    parameters: " + JSON.stringify(parameters));

    const rows = await sendPreparedStatementToInfomart(query, parameters)
    console.log("[agent-outputs] result: " + JSON.stringify(rows));
    return rows;
    
};

const getInboundOutputs = async ({ date_unit, tenant_key, site_cd, agent_group, agent_id, start_date, end_date }) => {

    const main_view_name = getViewName(date_unit);
    const date_type = getDateType(date_unit);

    var agent_group_query = "";
    var media_type_query = "";
    var agent_id_query = "";
    // var interaction_type_query = "";

    if (isNotEmpty(param.agent_group)) {
        agent_group_query = ` AND B.GROUP_KEY = ${agent_group}`;
    }
    if (isNotEmpty(param.agent_id)) {
        agent_id_query = ` AND A.RESOURCE_KEY = ${agent_id}`;
    }
    if (isNotEmpty(param.media_type)) {
        media_type_query = ` AND A.MEDIA_TYPE_KEY = ${media_type}`;
    }
    // if (isNotEmpty(param.interaction_type)) {
    //     interaction_type_query = ` AND A.INTERACTION_TYPE_KEY IN (@interaction_type)`;
    //     interaction_type = param.interaction_type;
    // }

    // var parameters = {
    //     start_date: param.start_date,
    //     end_date: param.end_date,
    //     tenant_key: param.tenant_key,
    //     site_cd: param.site_cd,
    //     agent_group,
    //     agent_id,
    //     media_type,
    //     interaction_type
    // };
  
    const query = `
    SELECT DISTINCT *
          -- ROW_NUMBER() OVER(ORDER BY A.AGENT_NAME, A.DT_KEY, A.M_TYPE, A.I_TYPE) AS NUM
	      -- , A.DT_KEY , A.AGENT_NAME , A.M_TYPE , A.I_TYPE , A.OFFERED , A.FAILED , A.ENGAGE , A.ENGAGE_TIME , A.TRANSFER_INIT_AGENT , A.XFER_RECEIVED_ACCEPTED
	  FROM (SELECT (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
                    FROM   DATE_TIME DT
                    WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY
                    ) as DT_KEY, 
                   B.AGENT_NAME AS AGENT_NAME,
                   SUM(A.OFFERED) as IB_OFFERED, SUM(A.OFFERED) - SUM(A.ENGAGE) as IB_FAILED, SUM(A.ENGAGE) as IB_ENGAGE, SUM(A.ENGAGE_TIME) as IB_ENGAGE_TIME 
                   , SUM(A.TRANSFER_INIT_AGENT) as IB_TRANSFER_INIT_AGENT, SUM(A.XFER_RECEIVED_ACCEPTED) as IB_XFER_RECEIVED_ACCEPTED
           FROM ${main_view_name} A INNER JOIN
                   (SELECT DISTINCT A.RESOURCE_KEY, C.AGENT_FIRST_NAME as AGENT_NAME
                    FROM RESOURCE_GROUP_FACT_ A
                         INNER JOIN GROUP_ B ON (A.TENANT_KEY = B.TENANT_KEY AND A.GROUP_KEY = B.GROUP_KEY)
                         INNER JOIN RESOURCE_ C ON (A.TENANT_KEY = C.TENANT_KEY AND A.RESOURCE_KEY = C.RESOURCE_KEY)
                         INNER JOIN TENANT T ON (A.TENANT_KEY = T.TENANT_KEY)
                    WHERE B.GROUP_TYPE ='Agent'
                      AND ((A.START_DATE_TIME_KEY <= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${start_date})))
                           OR
                           (A.END_DATE_TIME_KEY >= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${end_date}))))
                      AND   A.ACTIVE_FLAG = 1
                      AND   T.TENANT_KEY = ${tenant_key}
                      AND   C.AGENT_LAST_NAME = ${site_cd}
                      ${agent_group_query}
                   ) B ON (A.RESOURCE_KEY = B.RESOURCE_KEY)
           WHERE  A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${start_date}))
                  AND (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${end_date}))
                  AND A.MEDIA_TYPE_KEY = 1
                  AND A.INTERACTION_TYPE_KEY IN (2, 8, 9, 10, 11, 19, 20, 21)
           ${agent_id_query}
           GROUP BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY, A.INTERACTION_TYPE_KEY
           ) A
    ; `
    ;

    const parameters = [];
  
    sendPreparedStatementToInfomart(query, parameters)
        .then(result => {
            res.status(200).json(result);
            console.log(
                `[agent-outputs.controller]: select success.`
            );
        })
        .catch(error => {
            console.log("query error:", error);
            res.status(500).send({
                message: "조회 도중 문제가 발생했습니다."
            });
            console.log(`[agent-outputs.controller]: select fail.`);
        });
    
};

const getOutboundOutputs = async ({ date_unit, tenant_key, site_cd, agent_group, agent_id, start_date, end_date }) => {

    const main_view_name = getViewName(date_unit);
    const date_type = getDateType(date_unit);

    var agent_group_query = "";
    var media_type_query = "";
    var agent_id_query = "";
    // var interaction_type_query = "";

    if (isNotEmpty(param.agent_group)) {
        agent_group_query = ` AND B.GROUP_KEY = ${agent_group}`;
    }
    if (isNotEmpty(param.agent_id)) {
        agent_id_query = ` AND A.RESOURCE_KEY = ${agent_id}`;
    }
    if (isNotEmpty(param.media_type)) {
        media_type_query = ` AND A.MEDIA_TYPE_KEY = ${media_type}`;
    }
    // if (isNotEmpty(param.interaction_type)) {
    //     interaction_type_query = ` AND A.INTERACTION_TYPE_KEY IN (@interaction_type)`;
    //     interaction_type = param.interaction_type;
    // }

    // var parameters = {
    //     start_date: param.start_date,
    //     end_date: param.end_date,
    //     tenant_key: param.tenant_key,
    //     site_cd: param.site_cd,
    //     agent_group,
    //     agent_id,
    //     media_type,
    //     interaction_type
    // };
  
    const query = `
    SELECT DISTINCT *
          -- ROW_NUMBER() OVER(ORDER BY A.AGENT_NAME, A.DT_KEY, A.M_TYPE, A.I_TYPE) AS NUM
	      -- , A.DT_KEY , A.AGENT_NAME , A.M_TYPE , A.I_TYPE , A.OFFERED , A.FAILED , A.ENGAGE , A.ENGAGE_TIME , A.TRANSFER_INIT_AGENT , A.XFER_RECEIVED_ACCEPTED
	  FROM (SELECT (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
                    FROM   DATE_TIME DT
                    WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY
                    ) as DT_KEY, 
                   B.AGENT_NAME AS AGENT_NAME,
                   SUM(A.OFFERED) as OB_OFFERED, SUM(A.OFFERED) - SUM(A.ENGAGE) as OB_FAILED, SUM(A.ENGAGE) as OB_ENGAGE, SUM(A.ENGAGE_TIME) as OB_ENGAGE_TIME 
                   , SUM(A.TRANSFER_INIT_AGENT) as OB_TRANSFER_INIT_AGENT, SUM(A.XFER_RECEIVED_ACCEPTED) as OB_XFER_RECEIVED_ACCEPTED
           FROM ${main_view_name} A INNER JOIN
                   (SELECT DISTINCT A.RESOURCE_KEY, C.AGENT_FIRST_NAME as AGENT_NAME
                    FROM RESOURCE_GROUP_FACT_ A
                         INNER JOIN GROUP_ B ON (A.TENANT_KEY = B.TENANT_KEY AND A.GROUP_KEY = B.GROUP_KEY)
                         INNER JOIN RESOURCE_ C ON (A.TENANT_KEY = C.TENANT_KEY AND A.RESOURCE_KEY = C.RESOURCE_KEY)
                         INNER JOIN TENANT T ON (A.TENANT_KEY = T.TENANT_KEY)
                    WHERE B.GROUP_TYPE ='Agent'
                      AND ((A.START_DATE_TIME_KEY <= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${start_date})))
                           OR
                           (A.END_DATE_TIME_KEY >= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${end_date}))))
                      AND   A.ACTIVE_FLAG = 1
                      AND   T.TENANT_KEY = ${tenant_key}
                      AND   C.AGENT_LAST_NAME = ${site_cd}
                      ${agent_group_query}
                   ) B ON (A.RESOURCE_KEY = B.RESOURCE_KEY)
           WHERE  A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${start_date}))
                  AND (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${end_date}))
                  AND A.MEDIA_TYPE_KEY = 1
                  AND A.INTERACTION_TYPE_KEY IN (3, 4, 5, 12, 13, 14, 15, 16, 17, 18, 22)
           ${agent_id_query}
           GROUP BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY, A.INTERACTION_TYPE_KEY
           ) A
    ; `
    ;

    const parameters = [];
  
    sendPreparedStatementToInfomart(query, parameters)
        .then(result => {
            res.status(200).json(result);
            console.log(
                `[agent-outputs.controller]: select success.`
            );
        })
        .catch(error => {
            console.log("query error:", error);
            res.status(500).send({
                message: "조회 도중 문제가 발생했습니다."
            });
            console.log(`[agent-outputs.controller]: select fail.`);
        });
    
};

const getInternalOutputs = async ({ date_unit, tenant_key, site_cd, agent_group, agent_id, start_date, end_date }) => {

    const main_view_name = getViewName(date_unit);
    const date_type = getDateType(date_unit);

    var agent_group_query = "";
    var media_type_query = "";
    var agent_id_query = "";
    // var interaction_type_query = "";

    if (isNotEmpty(param.agent_group)) {
        agent_group_query = ` AND B.GROUP_KEY = ${agent_group}`;
    }
    if (isNotEmpty(param.agent_id)) {
        agent_id_query = ` AND A.RESOURCE_KEY = ${agent_id}`;
    }
    if (isNotEmpty(param.media_type)) {
        media_type_query = ` AND A.MEDIA_TYPE_KEY = ${media_type}`;
    }
    // if (isNotEmpty(param.interaction_type)) {
    //     interaction_type_query = ` AND A.INTERACTION_TYPE_KEY IN (@interaction_type)`;
    //     interaction_type = param.interaction_type;
    // }

    // var parameters = {
    //     start_date: param.start_date,
    //     end_date: param.end_date,
    //     tenant_key: param.tenant_key,
    //     site_cd: param.site_cd,
    //     agent_group,
    //     agent_id,
    //     media_type,
    //     interaction_type
    // };
  
    const query = `
    SELECT DISTINCT *
          -- ROW_NUMBER() OVER(ORDER BY A.AGENT_NAME, A.DT_KEY, A.M_TYPE, A.I_TYPE) AS NUM
	      -- , A.DT_KEY , A.AGENT_NAME , A.M_TYPE , A.I_TYPE , A.OFFERED , A.FAILED , A.ENGAGE , A.ENGAGE_TIME , A.TRANSFER_INIT_AGENT , A.XFER_RECEIVED_ACCEPTED
	  FROM (SELECT (SELECT CONVERT(${date_type}, DT.CAL_DATE, 20)
                    FROM   DATE_TIME DT
                    WHERE  DT.DATE_TIME_KEY = A.DATE_TIME_KEY
                    ) as DT_KEY, 
                   B.AGENT_NAME AS AGENT_NAME,
                   SUM(A.OFFERED) as IN_OFFERED, SUM(A.OFFERED) - SUM(A.ENGAGE) as IN_FAILED, SUM(A.ENGAGE) as IN_ENGAGE, SUM(A.ENGAGE_TIME) as IN_ENGAGE_TIME 
                   , SUM(A.TRANSFER_INIT_AGENT) as IN_TRANSFER_INIT_AGENT, SUM(A.XFER_RECEIVED_ACCEPTED) as IN_XFER_RECEIVED_ACCEPTED
           FROM ${main_view_name} A INNER JOIN
                   (SELECT DISTINCT A.RESOURCE_KEY, C.AGENT_FIRST_NAME as AGENT_NAME
                    FROM RESOURCE_GROUP_FACT_ A
                         INNER JOIN GROUP_ B ON (A.TENANT_KEY = B.TENANT_KEY AND A.GROUP_KEY = B.GROUP_KEY)
                         INNER JOIN RESOURCE_ C ON (A.TENANT_KEY = C.TENANT_KEY AND A.RESOURCE_KEY = C.RESOURCE_KEY)
                         INNER JOIN TENANT T ON (A.TENANT_KEY = T.TENANT_KEY)
                    WHERE B.GROUP_TYPE ='Agent'
                      AND ((A.START_DATE_TIME_KEY <= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${start_date})))
                           OR
                           (A.END_DATE_TIME_KEY >= (SELECT DATE_TIME_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${end_date}))))
                      AND   A.ACTIVE_FLAG = 1
                      AND   T.TENANT_KEY = ${tenant_key}
                      AND   C.AGENT_LAST_NAME = ${site_cd}
                      ${agent_group_query}
                   ) B ON (A.RESOURCE_KEY = B.RESOURCE_KEY)
           WHERE  A.DATE_TIME_KEY BETWEEN (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${start_date}))
                  AND (SELECT DATE_TIME_HOUR_KEY FROM DATE_TIME WHERE CAL_DATE = CONVERT(datetime, ${end_date}))
                  AND A.MEDIA_TYPE_KEY = 1
                  AND A.INTERACTION_TYPE_KEY IN (1, 6, 7, 23)
           ${agent_id_query}
           GROUP BY B.AGENT_NAME, A.DATE_TIME_KEY, A.MEDIA_TYPE_KEY, A.INTERACTION_TYPE_KEY
           ) A
    ; `
    ;

    const parameters = [];
  
    sendPreparedStatementToInfomart(query, parameters)
        .then(result => {
            res.status(200).json(result);
            console.log(
                `[agent-outputs.controller]: select success.`
            );
        })
        .catch(error => {
            console.log("query error:", error);
            res.status(500).send({
                message: "조회 도중 문제가 발생했습니다."
            });
            console.log(`[agent-outputs.controller]: select fail.`);
        });
    
};

module.exports = {
    getAgentOutputs,
    getInboundOutputs,
    getOutboundOutputs,
    getInternalOutputs
}