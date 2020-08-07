const { 
    EnterQueue, 
    EnterQueueInfo,
    ConsultConnectQueue,
    ConsultConnectQueueInfo,
  } = require('../../../../../models/v4/queue');
  
  const { 
    leftJoinOnDIDResourceKey,
    leftJoinOnServiceResourceKey,
    summarizeServiceQueueData,
    addSummarizedServiceQueueData,
    addIvrProcessingColumnTo,
    addServiceConnectReqColumnZeroValueTo,
    mergeDIDAndServiceQueue,
    sortReceiveCallInfos,
  } = require('../../../../../utils/data-helper');
  
  const R = require('ramda');
  
  // const logger = require("../../../../../utils/logger")({
  //   dirname: "",
  //   filename: "",
  //   sourcename: "v3/receive-call-infos.controller.js"
  // });
  
  
  const readDatasource = ({ site_cd, date_unit, start_date, end_date, enter_queue_resource_keys, enter_queue_resource_key, consult_connect_queue_resource_keys, dnis_index, endpoint_index_keys }) => {
    return Promise.all([
      EnterQueue.find({ start_date, end_date, resource_keys: enter_queue_resource_keys, resource_key: enter_queue_resource_key, date_unit }),
      ConsultConnectQueue.find({ start_date, end_date, resource_keys: consult_connect_queue_resource_keys, date_unit }),
      EnterQueueInfo.findBySiteCd( { site_cd, end_date, endpoint_index_keys } ),
      ConsultConnectQueueInfo.findBySiteCd( { site_cd, end_date, endpoint_index_keys } )
    ])
  };
  
  const read = async (req, res, next) => {
    try {
      const date_unit = req.params.date_unit;
      const params = req.query || {};
      const [
        enter_queue_data, 
        consult_connect_queue_data,
        enter_queue_info_data,
        consult_connect_queue_info_data,
      ] = await readDatasource({
        ...params,
        date_unit,
      });
  
      console.log('[receive-call-infos.controller] enter_queue_data: ' + JSON.stringify(enter_queue_data));
      console.log('[receive-call-infos.controller] enter_queue_info_data: ' + JSON.stringify(enter_queue_info_data));
      console.log('[receive-call-infos.controller] consult_connect_queue_data: ' + JSON.stringify(consult_connect_queue_data));
      console.log('[receive-call-infos.controller] consult_connect_queue_info_data: ' + JSON.stringify(consult_connect_queue_info_data));
  
      const enter_queue_n_info_data = await leftJoinOnDIDResourceKey(enter_queue_data, enter_queue_info_data);
      console.log('[receive-call-infos.controller] enter_queue_n_info_data: ' + JSON.stringify(enter_queue_n_info_data));
  
      const consult_connect_queue_n_info_data = await leftJoinOnServiceResourceKey(consult_connect_queue_data, consult_connect_queue_info_data);
      console.log('[receive-call-infos.controller] consult_connect_queue_n_info_data: ' + JSON.stringify(consult_connect_queue_n_info_data));
  
      const summarized_connect_queue_data = await summarizeServiceQueueData(consult_connect_queue_n_info_data);
      console.log('[receive-call-infos.controller] summarized_connect_queue_data: ' + JSON.stringify(summarized_connect_queue_data));
  
      const final_enter_queue_data = await addSummarizedServiceQueueData(enter_queue_n_info_data, summarized_connect_queue_data);
      console.log('[receive-call-infos.controller] final_enter_queue_data: ' + JSON.stringify(final_enter_queue_data));
  
      const ivr_processing_column_added_data = await addIvrProcessingColumnTo(final_enter_queue_data);
      console.log('[receive-call-infos.controller] ivr_processing_column_added_data: ' + JSON.stringify(ivr_processing_column_added_data));
  
      const service_connect_req_column__zero_value_added_data = await addServiceConnectReqColumnZeroValueTo(ivr_processing_column_added_data);
      console.log('[receive-call-infos.controller] service_connect_req_column__zero_value_added_data: ' + JSON.stringify(service_connect_req_column__zero_value_added_data));
  
      const final_data = await mergeDIDAndServiceQueue(service_connect_req_column__zero_value_added_data, consult_connect_queue_n_info_data);
      console.log('[receive-call-infos.controller] final_data: ' + JSON.stringify(final_data));
      
      const sorted_data = await sortReceiveCallInfos(final_data);
      console.log('[receive-call-infos.controller] sorted_data: ' + JSON.stringify(sorted_data));
  
      res.status(200).json(sorted_data);
    } catch (error) {
      console.log(error);
      res.status(500).json({ errorCode: 500, errorMessage: '문제가 발생했습니다.' });
    }
  };
  
  module.exports = {
    read,
  }