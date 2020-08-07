const { forEachObjIndexed } = require('ramda');

const getResDataForNotMandatoryParam = (
  madatoryParamList = [],
  missingParamList = []
) => {
  return {
    success: false,
    message: `필수 파라미터 [${madatoryParamList.join(
      ","
    )}] 중  [ ${missingParamList.join(",")}] 가 누락되었습니다.`
  };
};

const promiseHandler = promise =>
  promise
    .then(data => ({
      data,
      err: null
    }))
    .catch(err => ({
      data: null,
      err
    }));

const decodeError = error =>
  `error.name:${error.name}|error.message:${error.message}`;

const responseError = (errorCode, errorMessage) => ({
  success: false,
  errorCode,
  errorMessage
});

const responseSuccess = rows => ({
  success: true,
  result: rows
});

const isEmpty = value => !value;

const checkMadatoryParameter = (value, key) => {
  if (isEmpty(value)) {
    const notExistMandatoryParameterError = new Error(`${key} 값이 없습니다.`);
    notExistMandatoryParameterError.code = '400';
    throw notExistMandatoryParameterError;
  }
  return false;
}

const checkMandatoryParameters = forEachObjIndexed(checkMadatoryParameter);

// SQL Injection 방어처리 [2019-10-07 추가]
const checkSqlInjection = (value) => {
  var result = {
    result: true,
    message: ""
  };
  if (value.search(/\s/)) {
    result.result = false;
    result.message = "Invalid Parameters";
  }
  return result;
}

// [2019-10-08] filter for SQL Injection Attack : numeric, :, -
const filterArgumentsNumeric = (args) => {
  return args.replace(/[^0-9:-]+/g , '');
}

// [2019-10-08] filter for SQL Injection Attack : numeric, :, -
const filterArgumentsNumericList = (args) => {
  return args.replace(/[^0-9,]+/g , '');
}

// [2019-10-08] filter for SQL Injection Attack : alphabet, numeric, :, -
const filterArguments = (args) => {
  return args.replace(/[^A-Za-z0-9:-]+/g , '');
}

// [2019-10-08] filter for SQL Injection Attack : alphabet, numeric, korean, :, -
const filterArgumentsIncludeKorean = (args) => {
  return args.replace(/[^A-Za-z0-9가-힣:-:_]+/g , '');
}

const filterDateArguments = (args) => {
  return args.replace(/[^(0-9)]/g , '');
}

const filterArgumentsNumber = (args) => {
  return args.replace(/[^0-9]/g , '');
}

module.exports = {
  getResDataForNotMandatoryParam,
  checkSqlInjection,
  filterArgumentsNumeric,
  filterArgumentsNumericList,
  filterArguments,
  filterArgumentsIncludeKorean,
  filterDateArguments,
  filterArgumentsNumber,
  promiseHandler,
  decodeError,
  responseError,
  responseSuccess,
  checkMandatoryParameters,
};
