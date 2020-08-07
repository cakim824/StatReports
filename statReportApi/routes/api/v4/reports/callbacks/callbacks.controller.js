// var logger = require("../../../../../utils/logger")({
//   dirname: "",
//   filename: "",
//   sourcename: "callbacks.controller.js"
// });

const { 
    getCallbackDatas
} = require('../../../../../models/v4/service/callbacks');

const {
  length,
  pluck,
  sum,
  prop
} = require('ramda')

const getSum = (data) => {

  var sum_data = new Object();

  var sumData = 0;

  sumData = sum(pluck('in_count', data));
  sum_data.in_count = sumData;

  sumData = sum(pluck('finish_count', data));
  sum_data.finish_count = sumData;

  sumData = sum(pluck('average_process_time', data));
  sum_data.average_process_time = sumData;

  sumData = sum(pluck('average_process_try_count', data));
  sum_data.average_process_try_count = sumData;

  return sum_data;
};

const getAvg = (raw_data, sum_data) => {

  var data_length;
  data_length = length(raw_data) == 0 ? 1 : length(raw_data);
  var avg_data = new Object();

  var avgData;

  avgData = prop('in_count', sum_data)/data_length;
  avg_data.in_count = avgData.toFixed(2); 

  avgData = prop('finish_count', sum_data)/data_length;
  avg_data.finish_count = avgData.toFixed(2); 

  avgData = prop('average_process_time', sum_data)/data_length;
  avg_data.average_process_time = avgData.toFixed(2); 

  avgData = prop('average_process_try_count', sum_data)/data_length;
  avg_data.average_process_try_count = avgData.toFixed(2); 

  return avg_data;
};

const read = async (req, res, next) => {
  try {
    
    const date_unit = req.params.date_unit;
    const param = req.query || {};

    const site_cd = param.site_cd;
    const start_date = param.start_date;
    const end_date = param.end_date;
    const in_type = param.in_type;
    const in_detail_type = param.in_detail_type;
    const dnis_index = param.dnis_index;
    const endpoint_index = param.endpoint_index;
    const start_time = param.start_time;
    const end_time = param.end_time;

    var callback_datas;

    callback_datas = await getCallbackDatas({ date_unit, site_cd, start_date, end_date, in_type, in_detail_type, dnis_index, endpoint_index, start_time, end_time });
    console.log('[callbacks.controller] callback_datas: ' + JSON.stringify(callback_datas));

    const sum_data = getSum(callback_datas);
    console.log('[callbacks.controller] sum_data: ' + JSON.stringify(sum_data));
      
    const avg_data = getAvg(callback_datas, sum_data);
    console.log('[callbacks.controller] avg_data: ' + JSON.stringify(avg_data));
 
    res.status(200).json({data: callback_datas, sum: sum_data, avg: avg_data});
  } catch (error) {
    console.log(error);
    res.status(500).json({ errorCode: 500, errorMessage: '문제가 발생했습니다.' });
  }
};

module.exports = {
  read,
}