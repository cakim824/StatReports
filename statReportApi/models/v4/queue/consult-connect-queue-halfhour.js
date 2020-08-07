/**
 * @author cakim@hansol.com
 */
const { sendPreparedStatementToInfomart } = require('../../../utils/mssql');
const { getTimestamp } = require('../../../utils/date');
const { addDtKeyColumnTo } = require('../../../utils/data-helper');
const { checkMandatoryParameters } = require('../../../utils/common');
const { filterArguments, filterArgumentsNumeric, filterArgumentsNumericList } = require('../../../utils/common');
// const logger = require("../../../utils/logger")({
//   dirname: "",
//   filename: "",
//   sourcename: "v4/consult-connect-queue-halfhour.js"
// });


const find = async ({ start_date, end_date, resource_keys, start_time, end_time }) => {

  var filteredStartDate = filterArguments(start_date);
  var filteredEndDate = filterArguments(end_date);
  var filteredResourceKeys = filterArgumentsNumericList(resource_keys);
  if ( filteredStartDate != start_date || filteredEndDate != end_date
      || filteredResourceKeys != resource_keys ) {
    console.log("[consult-connect-queue] arguments filtered: start_date=" + start_date + ", end_date=" + end_date + ", resource_keys=" + resource_keys );
  }
  var time_query = ''
  if ( start_time != "" ) {
    time_query = `  AND X.TIME_KEY BETWEEN '${start_time}' AND '${end_time}'`;
  }

  const mandatory_parameters = { 
    filteredStartDate, 
    filteredEndDate, 
    filteredResourceKeys 
  };
  checkMandatoryParameters(mandatory_parameters);

  const query = `
  SET ANSI_WARNINGS OFF
  SET ARITHIGNORE ON
  SET ARITHABORT OFF

  SELECT *
  FROM (
  SELECT 
		-- *
		TIME_GROUP, SERVICE_RESOURCE_KEY
		, TIME_GROUP*1800 AS DATE_TIME_KEY
		, LEFT((SELECT CONVERT(char(16), DT.CAL_DATE, 20) FROM   DATE_TIME DT WHERE  DT.DATE_TIME_KEY = TIME_GROUP*1800), 10) as DATE_KEY
		, RIGHT((SELECT CONVERT(char(16), DT.CAL_DATE, 20) FROM   DATE_TIME DT WHERE  DT.DATE_TIME_KEY = TIME_GROUP*1800), 5) as TIME_KEY
		, SUM(A.CONNECTED_AGENT_IN) AS CONNECTED_AGENT_IN
		, SUM(A.ACCEPTED_AGENT) AS ACCEPTED_AGENT
		, SUM(A.ABANDONED) AS ABANDONED
		, SUM(A.ABANDONED_INVITE) AS ABANDONED_INVITE
		, SUM(A.CLEARED) AS CLEARED
		, SUM(A.ETC) AS ETC
    , MAX(A.ACCEPTED_TIME_MAX) AS ACCEPTED_TIME_MAX
    , ISNULL(CONVERT(NUMERIC(13,2), ROUND( (SUM(A.ACCEPTED_AGENT) / SUM(A.CONNECTED_AGENT_IN) * 100) ,2)), 0.00) AS RESPONSE_RATE
  FROM (
    SELECT T1.DATE_TIME_KEY AS DATE_TIME_KEY
		    , (SELECT CONVERT(char(16), DT.CAL_DATE, 20) FROM   DATE_TIME DT WHERE  DT.DATE_TIME_KEY = T1.DATE_TIME_KEY) as DT_KEY
		    , FLOOR(T1.DATE_TIME_KEY / 1800) AS TIME_GROUP
        , T1.RESOURCE_KEY AS SERVICE_RESOURCE_KEY
        , SUM(T1.ENTERED) - SUM(T1.REDIRECTED) AS CONNECTED_AGENT_IN
        , SUM(T1.ACCEPTED_AGENT) AS ACCEPTED_AGENT
        , SUM(T1.ABANDONED) AS ABANDONED
        , SUM(T1.ABANDONED_INVITE) AS ABANDONED_INVITE
        , SUM(T1.CLEARED) AS CLEARED
        , SUM(T1.ROUTED_OTHER) AS ETC
        , MAX(T1.ACCEPTED_TIME_MAX) AS ACCEPTED_TIME_MAX
    FROM   AG2_QUEUE_SUBHR T1
    WHERE  T1.DATE_TIME_KEY BETWEEN @start_timestamp AND @end_timestamp
      AND    T1.RESOURCE_KEY IN (${filteredResourceKeys})
    GROUP BY T1.DATE_TIME_KEY, T1.RESOURCE_KEY
  ) A
  GROUP BY A.TIME_GROUP, SERVICE_RESOURCE_KEY
  ) X
  WHERE 1=1 
  ${time_query}
  ;`
  ;

  const parameter_types = {
    start_timestamp: "Int",
    end_timestamp: "Int"
  };

  const parameters = {
    start_timestamp: getTimestamp(filteredStartDate),
    end_timestamp:  getTimestamp(filteredEndDate)
  };

  console.log("[consult-connect-queue-halfhour] query: " + query + "\n    parameters: " + JSON.stringify(parameters));
  const rows = await sendPreparedStatementToInfomart(query, parameters, parameter_types);
  
  const date_format = "YYYY-MM-DD HH:mm";
  const dt_key_added_rows = await addDtKeyColumnTo(rows, date_format);

  return dt_key_added_rows;
};

module.exports = {
  find,
}