const dayjs = require('dayjs');
const {
  curry, length, type, map
} = require('ramda')

const default_date_format = 'YYYY-MM-DD HH:mm';

const convertDateFormat = date => {
  const YYYY = date.substring(0, 4);
  const MM = date.substring(4, 6);
  const DD = date.substring(6, 8);
  const HH = date.substring(8, 10);
  const mm = date.substring(10, 12);
  const ss = date.substring(12, 14);
  return `${YYYY}-${MM}-${DD}T${HH}:${mm}:${ss}`;
};

const getTodayDate = function() {
  var currentDate = new Date();

  var yyyy = currentDate.getFullYear().toString();
  var MM = (currentDate.getMonth() + 1).toString();
  var dd = currentDate.getDate().toString();

  return yyyy + '-' + (MM[1] ? MM : '0'+MM[0]) + '-' + (dd[1] ? dd : '0'+dd[0]);
};

const getTodayStartTime = function() {
  var currentDate = new Date();

  var yyyy = currentDate.getFullYear().toString();
  var MM = (currentDate.getMonth() + 1).toString();
  var dd = currentDate.getDate().toString();

  return yyyy + '-' + (MM[1] ? MM : '0'+MM[0]) + '-' + (dd[1] ? dd : '0'+dd[0]) + ' 00:00';
};

const getTodayEndTime = function() {
  var currentDate = new Date();

  var yyyy = currentDate.getFullYear().toString();
  var MM = (currentDate.getMonth() + 1).toString();
  var dd = currentDate.getDate().toString();

  return yyyy + '-' + (MM[1] ? MM : '0'+MM[0]) + '-' + (dd[1] ? dd : '0'+dd[0]) + ' 23:45';
};

const newDate = date => new Date(date);
const getDateDiff = (startDate, endDate) =>
  Math.abs(dayjs(endDate).diff(dayjs(startDate), 'second'));

const getTimestamp = date => dayjs(date).unix();
const _length = value => {
  const revisedValue = type(value) === 'Number' ? `${value}` : value;
  return length(revisedValue)
}
const isValidateTimestamp = timestamp => (_length(timestamp) === 10 || _length(timestamp) === 13);
const reviseTimestamp = timestamp => _length(timestamp) === 10 ? timestamp * 1000 : timestamp;
const getRevisedTimestamp = timestamp => {
  if (!isValidateTimestamp(timestamp)) {
    throw new Error('유효한 timestamp 값이 아닙니다.');
  }
  return reviseTimestamp(timestamp);
}

// console.log(
//   'timestampLength:', getRevisedTimestamp(1551366000)
// )
// console.log(
//   'timestampLength:', getRevisedTimestamp(155136600100)
// )

const getFormattedDateFromTimestamp = (timestamp, date_format = default_date_format) => {
  const revisedTimestamp = getRevisedTimestamp(timestamp);
  return dayjs(revisedTimestamp).format(date_format);
};


// console.log(
//   'getFormattedDateFromTimestamp:', getFormattedDateFromTimestamp(1551366000 * 1000)
// )
// console.log(
//   'getFormattedDateFromTimestamp:', getFormattedDateFromTimestamp(newDate(1551366000 * 1000))
// )
// console.log(
//   'getFormattedDateFromTimestamp:', getFormattedDateFromTimestamp(newDate(1553179500 * 1000))
// )
// console.log(
//   'getFormattedDateFromTimestamp:', getFormattedDateFromTimestamp('1551366000')
// )

// console.log(
//   length(`1553179500`)
// )

// const currentDate01 = '2019-03-01 15:00';
// console.log(
//   'getTimestamp:', getTimestamp(currentDate01)
// )

module.exports = {
  getTimestamp,
  getFormattedDateFromTimestamp,
  getTodayDate,
  getTodayStartTime,
  getTodayEndTime
};
