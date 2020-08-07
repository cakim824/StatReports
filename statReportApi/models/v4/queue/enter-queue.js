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
//   sourcename: "v4/enter-queue.js"
// });

const DATE_UNITS = {
  hourly: { 
    view_name: 'AG2_QUEUE_SUBHR',
    date_format: 'YYYY-MM-DD HH:mm',
  },
  daily: { 
    view_name: 'AG2_QUEUE_DAY',
    date_format: 'YYYY-MM-DD',
  },
  monthly: { 
    view_name: 'AG2_QUEUE_MONTH',
    date_format: 'YYYY-MM',
  }
}

const getViewName = date_unit => DATE_UNITS[date_unit].view_name;
const getDateFormat = date_unit => DATE_UNITS[date_unit].date_format;

const find = async ({ start_date, end_date, resource_keys, resource_key = 0, date_unit }) => {

  var filteredStartDate = filterArguments(start_date);
  var filteredEndDate = filterArguments(end_date);
  var filteredResourceKeys = filterArgumentsNumericList(resource_keys);
  var filteredResourceKey = filterArgumentsNumeric(resource_key);
  if ( filteredStartDate != start_date || filteredEndDate != end_date
      || filteredResourceKeys != resource_keys
      || filteredResourceKey != resource_key ) {
    console.log("[enter-queue-info] arguments filtered: start_date=" + start_date + ", end_date=" + end_date + ", resource_keys=" + resource_keys + ", resource_key=" + resource_key);
  }

  const view_name = getViewName(date_unit);
  const mandatory_parameters = { 
    filteredStartDate, 
    filteredEndDate, 
    filteredResourceKeys 
  };
  checkMandatoryParameters(mandatory_parameters);

  const resource_key_query = filteredResourceKey ? `AND    T1.RESOURCE_KEY = @resource_key` : '';

  const query = `
  SELECT T1.DATE_TIME_KEY
       , T1.RESOURCE_KEY AS DID_RESOURCE_KEY
  	   , ISNULL(SUM(T1.ENTERED + T1.CONSULT_ENTERED), 0) AS MAIN_CONTACT_CENTER_ENTERED
  FROM   ${view_name} T1
  WHERE  T1.DATE_TIME_KEY BETWEEN @start_timestamp AND @end_timestamp
  AND    T1.RESOURCE_KEY IN (${filteredResourceKeys})
  ${resource_key_query}
  GROUP BY T1.DATE_TIME_KEY, T1.RESOURCE_KEY
  ;
  `;

  const parameter_types = {
    start_timestamp: 'Int',
    end_timestamp: 'Int',
    resource_keys: 'Int',
    resource_key: 'Int'
  };

  const parameters = {
    start_timestamp: getTimestamp(filteredStartDate),
    end_timestamp:  getTimestamp(filteredEndDate),
    filteredResourceKeys,
    resource_key: filteredResourceKey || 0,
  }

  console.log("[enter-queue] query: " + query + "\n    parameters: " + JSON.stringify(parameters));

  const rows = await sendPreparedStatementToInfomart(query, parameters, parameter_types);

// console.log('[EnterQueue]rows:', rows)

  const date_format = getDateFormat(date_unit);
  const dt_key_added_rows = addDtKeyColumnTo(rows, date_format);

  return dt_key_added_rows;
};

module.exports = {
  find,
}