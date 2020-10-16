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
//   sourcename: "v4/consult-connect-queue.js"
// });

const DATE_UNITS = {
  quarter: { 
    view_name: 'AG2_QUEUE_SUBHR',
    date_format: 'YYYY-MM-DD HH:mm',
    date_type: "CHAR(10)"
  },
  hourly: { 
    view_name: 'AG2_QUEUE_HOUR',
    date_format: 'YYYY-MM-DD HH:mm',
    date_type: "CHAR(10)"
  },
  daily: { 
    view_name: 'AG2_QUEUE_DAY',
    date_format: 'YYYY-MM-DD',
    date_type: "CHAR(10)"
  },
  monthly: { 
    view_name: 'AG2_QUEUE_MONTH',
    date_format: 'YYYY-MM',
    date_type: "CHAR(7)"
  }
}

const getViewName = date_unit => DATE_UNITS[date_unit].view_name;
const getDateFormat = date_unit => DATE_UNITS[date_unit].date_format;
const getDateType = date_unit => DATE_UNITS[date_unit].date_type;
var isNotEmpty = value => value != "";

const find = async ({ start_date, end_date, resource_keys, date_unit, start_time, end_time }) => {

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

  const view_name = getViewName(date_unit);
  const date_type = getDateType(date_unit);
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
  SELECT T1.DATE_TIME_KEY
       , T1.RESOURCE_KEY AS SERVICE_RESOURCE_KEY
       , (SELECT CONVERT(${date_type}, (SELECT DATEADD(SECOND, T1.DATE_TIME_KEY, '01/01/1970 09:00:00')), 121)) AS DATE_KEY
       , (SELECT CONVERT(VARCHAR(5), (SELECT DATEADD(SECOND, T1.DATE_TIME_KEY, '01/01/1970 09:00:00')), 108)) AS TIME_KEY
  	   , SUM(T1.ENTERED) - SUM(T1.REDIRECTED) AS CONNECTED_AGENT_IN
       , SUM(T1.ACCEPTED_AGENT) AS ACCEPTED_AGENT
       , SUM(T1.ABANDONED) AS ABANDONED
       , SUM(T1.ABANDONED_INVITE) AS ABANDONED_INVITE
       , SUM(T1.CLEARED) AS CLEARED
       , SUM(T1.ROUTED_OTHER) AS ETC
       , MAX(T1.ACCEPTED_TIME_MAX) AS ACCEPTED_TIME_MAX
       , ISNULL(CONVERT(NUMERIC(13,2), ROUND((SUM(T1.ACCEPTED_AGENT) / (SUM(T1.ENTERED) - SUM(T1.REDIRECTED)) * 100) ,2)), 0.00) AS RESPONSE_RATE
  FROM   ${view_name} T1
  WHERE  T1.DATE_TIME_KEY BETWEEN @start_timestamp AND @end_timestamp
  AND    T1.RESOURCE_KEY IN (${filteredResourceKeys})
  GROUP BY T1.DATE_TIME_KEY, T1.RESOURCE_KEY
  ) X
  WHERE 1=1
  ${time_query}
  ;
  `;

  const parameter_types = {
    start_timestamp: "Int",
    end_timestamp: "Int"
  };

  const parameters = {
    start_timestamp: getTimestamp(filteredStartDate),
    end_timestamp:  getTimestamp(filteredEndDate)
  };

  console.log("[consult-connect-queue] query: " + query + "\n    parameters: " + JSON.stringify(parameters));
  const rows = await sendPreparedStatementToInfomart(query, parameters, parameter_types);
  
  const date_format = await getDateFormat(date_unit);
  const dt_key_added_rows = await addDtKeyColumnTo(rows, date_format);

  return dt_key_added_rows;
};

module.exports = {
  find,
}