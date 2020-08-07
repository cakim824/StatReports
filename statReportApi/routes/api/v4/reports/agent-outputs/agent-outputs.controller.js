var { sendPreparedStatementToInfomart } = require("../../../../../utils/mssql");
// var logger = require("../../../../../utils/logger")({
//   dirname: "",
//   filename: "",
//   sourcename: "agent-outputs.controller.js"
// });

const { 
  getAgentOutputs,
  getInboundOutputs,
  getOutboundOutputs,
  getInternalOutputs
} = require('../../../../../models/v4/agent/agent-outputs');

const {
  getInteractionTypeKeys
} = require('../../../../../models/v4/subquery/interaction-types.js');

const {
  length,
  pluck,
  sum,
  prop
} = require('ramda')

const { 
  outerJoinOnDtKeyAndAgentName,
  fulfillAgentOutputData
} = require('../../../../../utils/data-helper');


const getSum = (data) => {

  var sum_data = new Object();

  var sumData = 0;

  sumData = sum(pluck('IB_OFFERED', data));
  sum_data.IB_OFFERED = sumData;

  sumData = sum(pluck('IB_ENGAGE', data));
  sum_data.IB_ENGAGE = sumData;

  sumData = sum(pluck('OB_OFFERED', data));
  sum_data.OB_OFFERED = sumData;

  sumData = sum(pluck('OB_ENGAGE', data));
  sum_data.OB_ENGAGE = sumData;

  sumData = sum(pluck('IN_OFFERED', data));
  sum_data.IN_OFFERED = sumData;

  sumData = sum(pluck('IN_ENGAGE', data));
  sum_data.IN_ENGAGE = sumData;

  sumData = sum(pluck('TRANSFER_INIT_AGENT', data));
  sum_data.TRANSFER_INIT_AGENT = sumData;

  sumData = sum(pluck('XFER_RECEIVED_ACCEPTED', data));
  sum_data.XFER_RECEIVED_ACCEPTED = sumData;

  return sum_data;
};

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

    var inbound_type_keys = await getInteractionTypeKeys('INBOUND');
    var outbound_type_keys = await getInteractionTypeKeys('OUTBOUND');
    var internal_type_keys = await getInteractionTypeKeys('INTERNAL');

    var agent_output_data;
    var sum_data;

    agent_output_data = await getAgentOutputs({ date_unit, tenant_key, site_cd, agent_group, agent_id, start_date, end_date, start_time, end_time, inbound_type_keys, outbound_type_keys, internal_type_keys });
    console.log('[agent-outputs.controller] agent_output_data: ' + JSON.stringify(agent_output_data));
    sum_data = getSum(agent_output_data);
    console.log('[agent-outputs.controller] sum_data: ' + JSON.stringify(sum_data));
 
    res.status(200).json({data: agent_output_data, sum: sum_data});
  } catch (error) {
    console.log(error);
    res.status(500).json({ errorCode: 500, errorMessage: '문제가 발생했습니다.' });
  }
};

module.exports = {
  read
}