// var logger = require('../../../../../utils/logger')({ dirname: '', filename: '', sourcename: 'agent-login-infos-modified.controller.js' });

const { getTodayDate, getTodayStartTime, getTodayEndTime } = require('../../../../../utils/date');

const { 
  sortByAgentName,
  sortByAgentNameAndDate
} = require('../../../../../utils/data-helper');

const R = require('ramda');
const { union, merge, sortBy } = require('ramda');

const { 
  getSummaryData,
  getInfomartData
} = require('../../../../../models/v4/agent/agent-login-infos');



exports.readDaily = async (req, res, next) => {

  var param = JSON.parse(req.params.id);

  var tenant_key = param.tenant_key;
  var site_cd = param.site_cd;
  var agent_group = param.agent_group;
  var agent_id = param.agent_id;

  const start_date = param.start_date;
  const end_date = param.end_date;
  const start_date_day = param.start_date.substring(0, 10);
  const end_date_day = param.end_date.substring(0, 10);
  const today = getTodayDate();
  const today_start = getTodayStartTime();
  const today_end = getTodayEndTime();

  var final_data, sorted_data, summary_data, infomart_data;

  console.log(start_date_day);
  console.log(end_date_day);
  console.log(today);
  console.log(today_start);
  console.log(today_end);
      
  if((start_date_day >= today) && (end_date_day >= today)) {
    final_data = await getInfomartData({ tenant_key, site_cd, agent_group, agent_id, start_date, end_date });
    console.log("final_data: ", JSON.stringify(final_data));
    res.status(200).json(final_data);
  } else if((start_date_day < today) && (end_date_day < today)) {
    final_data = await getSummaryData({ tenant_key, site_cd, agent_group, agent_id, start_date, end_date });
    console.log("final_data: ", JSON.stringify(final_data));
    res.status(200).json(final_data);
  } else if((start_date_day < today) && (end_date_day >= today)) {
    summary_data = await getSummaryData({ tenant_key, site_cd, agent_group, agent_id, start_date, end_date: today_start });
    infomart_data = await getInfomartData({ tenant_key, site_cd, agent_group, agent_id, start_date: today_start, end_date: today_end });
    final_data = union(summary_data, infomart_data);
    console.log("final_data: ", JSON.stringify(final_data));
    sorted_data = await sortByAgentNameAndDate(final_data);
    console.log("sorted_data: ", JSON.stringify(sorted_data));
  
    for(var i=0; sorted_data[i]; i++) {
      merge(sorted_data, {'NUM': i});
    }
    console.log("sorted_data: ", JSON.stringify(sorted_data));
    res.status(200).json(sorted_data);
  }

};