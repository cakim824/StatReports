const { 
    EnterQueueInfo,
    ConsultConnectQueue,
    ConsultConnectQueueHalfhour,
    ConsultConnectQueueInfo,
  } = require('../../../../../models/v4/queue');
  
  const { 
    leftJoinOnServiceResourceKey,
    leftJoinOnPrevResourceKeyAndDIDResourceKey,
    filterByKeyNumber,
    filterByServiceName,
    filterTwoSlot,
    filterNone,
    sortReceiveCallInfos,
  } = require('../../../../../utils/data-helper');

  const {
    length,
    pluck,
    sum,
    prop,
    groupBy,
    reverse,
    sort
  } = require('ramda')
  
  // const logger = require("../../../../../utils/logger")({
  //   dirname: "",
  //   filename: "",
  //   sourcename: "v4/service-infos.controller.js"
  // });
  
  const readDatasource = ({ start_date, end_date, consult_connect_queue_resource_keys, site_cd, date_unit, search_type, dnis_index, endpoint_index, endpoint_index_keys, time_unit, start_time, end_time }) => {
    if(time_unit == 30) {
      return Promise.all([
        EnterQueueInfo.findBySiteCd({ site_cd, end_date, endpoint_index_keys }),
        ConsultConnectQueueHalfhour.find({ start_date, end_date, resource_keys: consult_connect_queue_resource_keys, start_time, end_time }),
        ConsultConnectQueueInfo.findBySiteCd({ site_cd, end_date, endpoint_index_keys })
      ])
    }
    if(time_unit == 15) {
      return Promise.all([
        EnterQueueInfo.findBySiteCd({ site_cd, end_date, endpoint_index_keys }),
        ConsultConnectQueue.find({ start_date, end_date, resource_keys: consult_connect_queue_resource_keys, date_unit: "quarter", start_time, end_time }),
        ConsultConnectQueueInfo.findBySiteCd({ site_cd, end_date, endpoint_index_keys })
      ])
    }

    return Promise.all([
      EnterQueueInfo.findBySiteCd({ site_cd, end_date, endpoint_index_keys }),
      ConsultConnectQueue.find({ start_date, end_date, resource_keys: consult_connect_queue_resource_keys, date_unit, start_time, end_time }),
      ConsultConnectQueueInfo.findBySiteCd({ site_cd, end_date, endpoint_index_keys })
    ])
  };

  const byKeyNumber = groupBy(function (data) {
    const keyNumber = data.KEY_NUMBER;
    const dtKey = data.DT_KEY;
    return [keyNumber, dtKey];
  })

  const getGroupBySum = (data) => {

    var groupby_sum = new Array;
    var groupby_data = byKeyNumber(data);
    var groupby_data_keys = Object.keys(groupby_data);
    var group_data_length = length(groupby_data_keys);

    for (var i = 0; i < group_data_length; i++) {
      var sumData, queueData;
      var tmp_obj = new Object();
      var tmp_key = groupby_data_keys[i];

      // queueData = pluck('DT_KEY', groupby_data[tmp_key]);
      // tmp_obj.DT_KEY = queueData;
      // queueData = pluck('KEY_NUMBER', groupby_data[tmp_key]);
      // tmp_obj.KEY_NUMBER = queueData;

      tmp_obj.DT_KEY = tmp_key.split(',')[1];
      tmp_obj.KEY_NUMBER = tmp_key.split(',')[0];
      tmp_obj.SERVICE_NAME = '전체';
      tmp_obj.DATE_KEY = pluck('DATE_KEY', groupby_data[tmp_key])[0];
      tmp_obj.TIME_KEY = pluck('TIME_KEY', groupby_data[tmp_key])[0];

      sumData = sum(pluck('CONNECTED_AGENT_IN', groupby_data[tmp_key]));
      tmp_obj.CONNECTED_AGENT_IN = sumData;
      sumData = sum(pluck('ACCEPTED_AGENT', groupby_data[tmp_key]));
      tmp_obj.ACCEPTED_AGENT = sumData;
      sumData = sum(pluck('ABANDONED', groupby_data[tmp_key]));
      tmp_obj.ABANDONED = sumData;
      sumData = sum(pluck('ABANDONED_INVITE', groupby_data[tmp_key]));
      tmp_obj.ABANDONED_INVITE = sumData;
      sumData = sum(pluck('CLEARED', groupby_data[tmp_key]));
      tmp_obj.CLEARED = sumData;
      sumData = sum(pluck('ETC', groupby_data[tmp_key]));
      tmp_obj.ETC = sumData;
      sumData = sort(function(a, b) { return b - a }, pluck('ACCEPTED_TIME_MAX', groupby_data[tmp_key]));
      tmp_obj.ACCEPTED_TIME_MAX = sumData[0];
      sumData = sum(pluck('ACCEPTED_AGENT', groupby_data[tmp_key]))/sum(pluck('CONNECTED_AGENT_IN', groupby_data[tmp_key])) *100;	
      tmp_obj.RESPONSE_RATE = sumData.toFixed(2);

      groupby_sum.push(tmp_obj)
    }

    return groupby_sum;
  };


  const getSum = (data) => {

    var sum_data = new Object();

    var sumData = 0;

    sumData = sum(pluck('CONNECTED_AGENT_IN', data));
    sum_data.CONNECTED_AGENT_IN = sumData;

    sumData = sum(pluck('ACCEPTED_AGENT', data));
    sum_data.ACCEPTED_AGENT = sumData;

    sumData = sum(pluck('ABANDONED', data));
    sum_data.ABANDONED = sumData;

    sumData = sum(pluck('ABANDONED_INVITE', data));
    sum_data.ABANDONED_INVITE = sumData;

    sumData = sum(pluck('CLEARED', data));
    sum_data.CLEARED = sumData;

    sumData = sum(pluck('ETC', data));
    sum_data.ETC = sumData;


    return sum_data;
  };

  const getAvg = (sorted_data, sum_data) => {

    var data_length;
    data_length = length(sorted_data) == 0 ? 1 : length(sorted_data);
    var avg_data = new Object();

    var avgData;

    avgData = prop('CONNECTED_AGENT_IN', sum_data)/data_length;
    avg_data.CONNECTED_AGENT_IN = avgData.toFixed(2); 

    avgData = prop('ACCEPTED_AGENT', sum_data)/data_length;
    avg_data.ACCEPTED_AGENT = avgData.toFixed(2); 

    avgData = prop('ABANDONED', sum_data)/data_length;
    avg_data.ABANDONED = avgData.toFixed(2); 

    avgData = prop('ABANDONED_INVITE', sum_data)/data_length;
    avg_data.ABANDONED_INVITE = avgData.toFixed(2); 

    avgData = prop('CLEARED', sum_data)/data_length;
    avg_data.CLEARED = avgData.toFixed(2); 

    avgData = prop('ETC', sum_data)/data_length;
    avg_data.ETC = avgData.toFixed(2); 

    avgData = prop('ACCEPTED_AGENT', sum_data) / prop('CONNECTED_AGENT_IN', sum_data) *100;	
    avg_data.RESPONSE_RATE = avgData.toFixed(2);

    return avg_data;
  };
  
  const read = async (req, res, next) => {
    try {
      const { date_unit } = req.params || {};
      const params = req.query || {};

      const [
        enter_queue_info_data,
        consult_connect_queue_data,
        consult_connect_queue_info_data
      ] = await readDatasource({
        ...params,
        date_unit,
      });

      console.log('[service-infos.controller] enter_queue_info_data: ' + JSON.stringify(enter_queue_info_data));
      console.log('[service-infos.controller] consult_connect_queue_data: ' + JSON.stringify(consult_connect_queue_data));
      console.log('[service-infos.controller] consult_connect_queue_info_data: ' + JSON.stringify(consult_connect_queue_info_data));
  
      const consult_connect_queue_n_info_data = await leftJoinOnServiceResourceKey(consult_connect_queue_data, consult_connect_queue_info_data);
      console.log('[service-infos.controller] consult_connect_queue_n_info_data: ' + JSON.stringify(consult_connect_queue_n_info_data));
      const consult_connect_queue_n_info_n_enter_queue_info_data = await leftJoinOnPrevResourceKeyAndDIDResourceKey(consult_connect_queue_n_info_data, enter_queue_info_data);
      console.log('[service-infos.controller] consult_connect_queue_n_info_n_enter_queue_info_data: ' + JSON.stringify(consult_connect_queue_n_info_n_enter_queue_info_data));
  
      const filterBySearchParameter = filterTwoSlot(
        params.dnis_index ? filterByKeyNumber(params.dnis_index) : filterNone,
        params.endpoint_index ? filterByServiceName(params.endpoint_index) : filterNone
      )
  
      const filtered_data = filterBySearchParameter(consult_connect_queue_n_info_data);
      console.log('[service-infos.controller] filtered_data: ' + JSON.stringify(filtered_data));
      const sorted_data = await sortReceiveCallInfos(filtered_data);
      console.log('[service-infos.controller] sorted_data: ' + JSON.stringify(sorted_data));

      const group_sum_data = getGroupBySum(sorted_data);
      console.log('[service-infos.controller] group_sum_data: ' + JSON.stringify(group_sum_data));

      const sum_data = getSum(sorted_data);
      console.log('[service-infos.controller] sum_data: ' + JSON.stringify(sum_data));
      const avg_data = getAvg(sorted_data, sum_data);
      console.log('[service-infos.controller] avg_data: ' + JSON.stringify(avg_data));
  

      if(params.search_type == "group") {
        res.status(200).json({data: group_sum_data, sum: sum_data, avg: avg_data});
      }
      else {
        res.status(200).json({data: sorted_data, sum: sum_data, avg: avg_data});
      }
      
    } catch (error) {
      if (error.code === '400') {
        res.status(error.code).json({ errorCode: error.code, errorMessage: error.message });  
      }
      console.log(error);
      res.status(500).json({ errorCode: 500, errorMessage: '문제가 발생했습니다.' });
    }
  };

  module.exports = {
    read,
  }
  