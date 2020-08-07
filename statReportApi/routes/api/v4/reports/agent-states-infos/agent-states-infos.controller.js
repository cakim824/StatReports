var { sendPreparedStatementToInfomart } = require("../../../../../utils/mssql");
// var logger = require("../../../../../utils/logger")({
//   dirname: "",
//   filename: "",
//   sourcename: "agent-states-infos.controller.js"
// });

const { 
  getAgentStatesInfos,
  getAgentLoginInfos
} = require('../../../../../models/v4/agent/agent-states-infos');

const { 
    joinOnDateTimeKeyAndAgentName
} = require('../../../../../utils/data-helper');

const read = async (req, res, next) => {
  try {
    
    const date_unit = req.params.date_unit;
    const param = req.query || {};

    const tenant_key = param.tenant_key;
    const site_cd = param.site_cd;
    const agent_group = param.agent_group;
    const agent_id = param.agent_id;
    const start_date = param.start_date;
    const end_date = param.end_date;
    
    const start_time = param.start_time;
    const end_time = param.end_time;

    var agent_states_data;
    var agent_login_data;
    var final_data;

    agent_states_data = await getAgentStatesInfos({ date_unit, tenant_key, site_cd, agent_group, agent_id, start_date, end_date, start_time, end_time });

    if (date_unit == "daily") {
        agent_login_data = await getAgentLoginInfos({ date_unit, tenant_key, site_cd, agent_group, agent_id, start_date, end_date });
        final_data = await joinOnDateTimeKeyAndAgentName(agent_states_data, agent_login_data);
        console.log("[agent-login-infos-daily] fianl_data: " + JSON.stringify(final_data));
        res.status(200).json(final_data);
    }
 
    res.status(200).json(agent_states_data);
  } catch (error) {
    console.log(error);
    res.status(500).json({ errorCode: 500, errorMessage: '문제가 발생했습니다.' });
  }
};

module.exports = {
  read
}