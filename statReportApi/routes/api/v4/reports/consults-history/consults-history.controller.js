var { sendPreparedStatementToInfomart } = require("../../../../../utils/mssql");
// var logger = require("../../../../../utils/logger")({
//   dirname: "",
//   filename: "",
//   sourcename: "agent-outputs.controller.js"
// });

const { 
    getConsultsHistory
} = require('../../../../../models/v4/consult/consults-history');

const {
    getEmpnoList
} = require('../../../../../models/v4/subquery/empno-list.js');


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
    const media_type = param.media_type;
    const interaction_type = param.interaction_type;
    const main_category = param.main_category;
    const medium_category = param.medium_category;
    const sub_category = param.sub_category;

    var empno_list = '';
    if(agent_group != '') {
        var empno_list = await getEmpnoList({ agent_group, site_cd });
    }

    var consults_history_data;

    consults_history_data = await getConsultsHistory({ date_unit, tenant_key, site_cd, agent_group, agent_id, empno_list, start_date, end_date, start_time, end_time, media_type, interaction_type, main_category, medium_category, sub_category });
    console.log('[consults-history.controller] consults_history_data: ' + JSON.stringify(consults_history_data));
 
    res.status(200).json({data: consults_history_data});
  } catch (error) {
    console.log(error);
    res.status(500).json({ errorCode: 500, errorMessage: '문제가 발생했습니다.' });
  }
};

module.exports = {
  read
}