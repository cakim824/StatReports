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
} = require('../../../../../models/v4/agent/agent-total-outputs');

const {
  getInteractionTypeKeys
} = require('../../../../../models/v4/subquery/interaction-types.js');

const {
    getTenantKey
} = require('../../../../../models/v4/subquery/tenant-key.js');


const { 
  outerJoinOnDtKeyAndAgentName,
  fulfillAgentOutputData
} = require('../../../../../utils/data-helper');

const read = async (req, res, next) => {
  try {
    
    const date_unit = req.params.date_unit;
    const param = req.query || {};

    // const interaction_type = param.interaction_type;
    // const tenant_key = param.tenant_key;
    const site_cd = param.site_cd;
    const start_date = param.start_date;
    const end_date = param.end_date;
    const start_time = param.start_time;
    const end_time = param.end_time;

    var tenant_key = await getTenantKey(site_cd);
    var inbound_type_keys = await getInteractionTypeKeys('INBOUND');
    var outbound_type_keys = await getInteractionTypeKeys('OUTBOUND');
    var internal_type_keys = await getInteractionTypeKeys('INTERNAL');

    var total_output_data;
    total_output_data = await getAgentOutputs({ date_unit, tenant_key, site_cd, start_date, end_date, start_time, end_time, inbound_type_keys, outbound_type_keys });

    // if(interaction_type == 'inbound') {
    //     total_output_data = await getInboundOutputs({ date_unit, tenant_key, site_cd, start_date, end_date, start_time, end_time }); 
    // } else if(interaction_type == 'outbound') {
    //     total_output_data = await getOutboundOutputs({ date_unit, tenant_key, site_cd, start_date, end_date, start_time, end_time }); 
    // } else if(interaction_type == 'internal') {
    //     total_output_data = await getInternalOutputs({ date_unit, tenant_key, site_cd, start_date, end_date, start_time, end_time }); 
    // }

    console.log('[agent-total-outputs.controller] total_output_data: ' + JSON.stringify(total_output_data));
    res.status(200).json({data: total_output_data});
  } catch (error) {
    console.log(error);
    res.status(500).json({ errorCode: 500, errorMessage: '문제가 발생했습니다.' });
  }
};

module.exports = {
  read
}