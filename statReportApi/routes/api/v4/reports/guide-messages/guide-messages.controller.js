// var logger = require("../../../../../utils/logger")({
//     dirname: "",
//     filename: "",
//     sourcename: "guide-messages.controller.js"
//   });
  
const { 
    getMsgRequestData
} = require('../../../../../models/v4/service/guide-messages');

const {
  pluck,
  sum
} = require('ramda')

const getSum = (data) => {

  var sum_data = new Object();

  var sumData = 0;

  sumData = sum(pluck('COUNT', data));
  sum_data.COUNT = sumData;

  return sum_data;
};

const read = async (req, res, next) => {
  try {
    
    const date_unit = req.params.date_unit;
    const param = req.query || {};

    const site_cd = param.site_cd;
    const start_date = param.start_date;
    const end_date = param.end_date;
    const dnis = param.dnis;
    
    const start_time = param.start_time;
    const end_time = param.end_time;

    var msg_request_data;

    msg_request_data = await getMsgRequestData({ date_unit, site_cd, start_date, end_date, dnis, start_time, end_time });
    console.log('[guide-messages.controller] msg_request_data: ' + JSON.stringify(msg_request_data));

    const sum_data = getSum(msg_request_data);
    console.log('[guide-messages.controller] sum_data: ' + JSON.stringify(sum_data));
 
    res.status(200).json({data: msg_request_data, sum: sum_data});
  } catch (error) {
    console.log(error);
    res.status(500).json({ errorCode: 500, errorMessage: '문제가 발생했습니다.' });
  }
};

module.exports = {
  read,
}