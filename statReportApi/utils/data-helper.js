const {
    reduce,
    map,
    curry,
    indexBy,
    values,
    merge,
    mergeWith,
    isNil,
    assoc,
    prop,
    props,
    forEach,
    groupBy,
    split,
    pipe,
    pluck,
    toPairs,
    filter,
    tap,
    sortBy,
    sortWith,
    ascend
  } = require("ramda");
  
  const dayjs = require('dayjs');
  const fecha = require('fecha');
  const { getFormattedDateFromTimestamp } = require('./date.js');
  
  const pass = tap(() => '');
  
  const sum = reduce((a, b) => a + b, 0);
  const groupByKeys = (...cols) => row => map(col => row[col], cols).join("|");
  
  const joinRight = curry((mapper1, mapper2, t1, t2) => {
    let indexed = indexBy(mapper1, t1);
    return t2.map(t2row => merge(t2row, indexed[mapper2(t2row)]));
  });
  
  const joinLeft = curry((f1, f2, t1, t2) => {
    return joinRight(f2, f1, t2, t1);
  });
  
  const joinInner = curry((f1, f2, t1, t2) => {
    let indexed = indexBy(f1, t1);
    return chain(t2row => {
      let corresponding = indexed[f2(t2row)];
      return corresponding ? [merge(t2row, corresponding)] : [];
    }, t2);
  });
  
  const joinOuter = curry((f1, f2, t1, t2) => {
    let o1 = indexBy(f1, t1);
    let o2 = indexBy(f2, t2);
    return values(mergeWith(merge, o1, o2));
  });
  
  const nestedJoin = curry((f1, f2, newCol, t1, t2) => {
    let indexed = indexBy(f1, t1);
    return t2.map(t2row => {
      let corresponding = indexed[f2(t2row)];
      return isNil(corresponding)
        ? t2row
        : assoc(newCol, corresponding, t2row);
    });
  });
  
  const onDIDResourceKey = prop("DID_RESOURCE_KEY");
  const onServiceResourceKey = prop("SERVICE_RESOURCE_KEY");
  const onPrevResourceKey = prop("PREV_RESOURCE_KEY");
  const onDTKeyAndDIDResourceKey = props(["DT_KEY", "DID_RESOURCE_KEY"]);
  const onDTKeyAndPrevResourceKey = props(["DT_KEY", "PREV_RESOURCE_KEY"]);
  const onDateTimeKey = prop("DATE_TIME_KEY");   // {2019-08-07 추가}
  
  const addMetaData = curry(
    (originTableJoinKey, metaTableJoinKey, originTable, metaTable) => {
      return joinLeft(
        originTableJoinKey,
        metaTableJoinKey,
        originTable,
        metaTable
      );
    }
  );
  
  const leftJoinOnDIDResourceKey = (left_table, right_table) => new Promise((resolve, reject) => {
    resolve(addMetaData(onDIDResourceKey, onDIDResourceKey)(left_table, right_table));
  });
  
  const leftJoinOnServiceResourceKey = (left_table, right_table) => new Promise((resolve, reject) => {
    resolve(addMetaData(onServiceResourceKey, onServiceResourceKey)(left_table, right_table));
  });
  
  const leftJoinOnPrevResourceKeyAndDIDResourceKey = (left_table, right_table) => new Promise((resolve, reject) => {
    resolve(addMetaData(onPrevResourceKey, onDIDResourceKey)(left_table, right_table));
  });
  
  // {2019-08-07 추가}
  const leftJoinOnDateTimeKey = (left_table, right_table) => new Promise((resolve, reject) => {
    resolve(addMetaData(onDateTimeKey, onDateTimeKey)(left_table, right_table));
  });
  
  const addSummarizedServiceQueueData = (left_table, right_table) => new Promise((resolve, reject) => {
    resolve(addMetaData(onDTKeyAndDIDResourceKey, onDTKeyAndPrevResourceKey)(left_table, right_table));
  });
  
  const calculateIvrProcessing = data => {
    const { MAIN_CONTACT_CENTER_ENTERED = 0, SERVICE_CONNECT_REQ = 0 } = data
    // return MAIN_CONTACT_CENTER_ENTERED - SERVICE_CONNECT_REQ
    return (MAIN_CONTACT_CENTER_ENTERED > SERVICE_CONNECT_REQ) ? (MAIN_CONTACT_CENTER_ENTERED - SERVICE_CONNECT_REQ) : 0
  }
  
  const addIvrProcessing = data => ({ ...data, IVR_PROCESSING: calculateIvrProcessing(data) })
  const addServiceConnectReqZeroValue = data => {
    return { ...data, SERVICE_CONNECT_REQ: data.SERVICE_CONNECT_REQ || 0 }
  }
  
  const addIvrProcessingColumnTo = data => new Promise((resolve, reject) => {
    resolve(map(addIvrProcessing, data));
  });
  const addServiceConnectReqColumnZeroValueTo = data => new Promise((resolve, reject) => {
    resolve(map(addServiceConnectReqZeroValue, data));
  });
  
  const on = curry((rowAFn, rowBFn, rowA, rowB) => {
    return rowAFn(rowA) === rowBFn(rowB);
  });
  
  const didQueueKeys = groupByKeys("DID_RESOURCE_KEY", "DT_KEY");
  const serviceQueueKeys = groupByKeys("PREV_RESOURCE_KEY", "DT_KEY");
  const onDIDQueueAndServiceQueue = on(didQueueKeys, serviceQueueKeys);
  
  const nestedLoopJoin = curry((pred, tblA, tblB) => {
    let resData = [];
    forEach(outerTblRow => {
      let notExistMatchRow = true;
      forEach(innerTblRow => {
        let mergedData = {};
        if (pred(outerTblRow, innerTblRow)) {
          resData.push(merge(outerTblRow, innerTblRow));
          notExistMatchRow = false;
        }
      }, tblB);
      if (notExistMatchRow) {
        resData.push(outerTblRow);
      }
    }, tblA);
    return resData;
  });
  
  const mergeDIDAndServiceQueue = (left_table, right_table) => new Promise((resolve, reject) => {
    resolve(nestedLoopJoin(onDIDQueueAndServiceQueue)(left_table, right_table));
  });
  
  
  const splitArrayDataToLabelAndResult = map(data => {
    const [label, result] = data;
    return {
      label: label,
      result: result
    };
  });
  
  const groupByDTKeyAndPrevResourceKey = groupBy(
    groupByKeys("DT_KEY", "PREV_RESOURCE_KEY")
  );
  
  const aggregateData = map(data => {
    const [DT_KEY, PREV_RESOURCE_KEY] = split("|", data.label);
    const getDataForSum = pluck("CONNECTED_AGENT_IN");
    const sumData = sum(getDataForSum(data.result));
    return {
      DT_KEY,
      PREV_RESOURCE_KEY,
      SERVICE_CONNECT_REQ: sumData
    };
  });
  
  const summarizeServiceQueueData = service_queue_data => new Promise((resolve, reject) => {
    resolve(
      pipe(
        groupByDTKeyAndPrevResourceKey,
        toPairs,
        splitArrayDataToLabelAndResult,
        aggregateData
      )(service_queue_data)
    );
  });
  
  const addDtKeyColumn = curry((date_format, data) => {
    return {
      ...data,
      DT_KEY: getFormattedDateFromTimestamp(data.DATE_TIME_KEY, date_format)
    }
  });
  const addDtKeyColumnTo = (data, date_format) => map(addDtKeyColumn(date_format), data);
  
  const filterConsultConnectQueueBy = queue_key => filter(data => data.DID_RESOURCE_KEY == queue_key);
  const filterConsultConnectQueueByConsultQueueKey = queue_key => filter(data => data.SERVICE_RESOURCE_KEY == queue_key);
  const filterTwoSlot = (slot1 = pass, slot2 = pass) => pipe(
    slot1,
    slot2,
  );
  
  const filterBy = curry((standard_column, queue_key) => filter(data => data[standard_column] == queue_key));
  const filterNone = filter(() => true);
  const filterByEnterQueue = filterBy('DID_RESOURCE_KEY');
  const filterByConsultConnectQueue = filterBy('SERVICE_RESOURCE_KEY');
  const filterByKeyNumber = filterBy('KEY_NUMBER_INDEX');       // {2019-09-25 추가}
  const filterByServiceName = filterBy('SERVICE_NAME_INDEX');   // {2019-09-25 추가}
  
  // {2020-05-18 추가}
  const agentLoginInfosSortKey = data => `${data.AGENT_NAME}`;
  const sortByAgentName = data => new Promise((resolve, reject) => {
    resolve(
      sortBy(agentLoginInfosSortKey, data)
    );
  });
  
  // {2020-05-18 추가}
  const sortByAgentNameAndDate = data => new Promise((resolve, reject) => {
    resolve(
      sortWith([ascend(prop("AGENT_NAME")), ascend(prop("DT_KEY"))], data)
    );
  });
  
  const receiveCallInfosSortKey = data => `${data.DATE_TIME_KEY}|${data.KEY_NUMBER}|${data.SERVICE_NAME}`;
  const sortReceiveCallInfos = data => new Promise((resolve, reject) => {
    resolve(
      sortBy(receiveCallInfosSortKey, data)
    );
  });
  
  // [2019-09-25] added for service info MISO
  const appendReqAndProCountField = function (x) {
    x.REQCOUNT = 0;
    x.PROCOUNT = 0;
    return x;
  };
  
  const appendCallbackItem = function (data) {
    const prodata = forEach(appendReqAndProCountField, data);
    return prodata;
  };
  
  const onDTKey = prop("DT_KEY");
  const onDTKeyAndSvcResourceKey = props(["DT_KEY", "SERVICE_RESOURCE_KEY"]);
  const onDatTimeKeyAndSvcResourceKey = props(["DATE_TIME_KEY", "SERVICE_RESOURCE_KEY"]);
  const onDateTimeAndSvcResrcKey = prop("DTSVCRKEY");
  
  const outerJoinOnDateTimeAndSvcResrcKey = (left_table, right_table) => {
    return joinOuter(onDateTimeAndSvcResrcKey, onDateTimeAndSvcResrcKey, left_table, right_table);
  };
  
  const DTKeySvcRsrcKeys = groupByKeys("DT_KEY", "SERVICE_RESOURCE_KEY");
  const DateTimeKeySvcRsrcKeys = groupByKeys("DATE_TIME_KEY", "SERVICE_RESOURCE_KEY");
  
  const outerJoinOnDTKeyAndDateTimeKey = (left_table, right_table) => new Promise((resolve, reject) => {
    //  resolve(joinOuter(onDTKeyAndSvcResourceKey, onDatTimeKeyAndSvcResourceKey, left_table, right_table));
      resolve(joinOuter(onDTKeyAndSvcResourceKey, onDTKeyAndSvcResourceKey, left_table, right_table)); // temporary
     });
    
  const fullfillServiceData = function (x, data_format) {
    if ( typeof x.CONNECTED_AGENT_IN === "undefined" ) {
      x.CONNECTED_AGENT_IN = 0;
      x.ACCEPTED_AGENT = 0;
      x.ABANDONED = 0;
      x.ABANDONED_INVITE = 0;
      x.CLEARED = 0;
      x.ETC = 0;
      x.ACCEPTED_TIME_MAX = 0;
    } 
    return x;
  }; 
  
  const fullfillServiceMisoData = function (data, data_format) {
    const fullfilleddata = forEach(fullfillServiceData, data, data_format);
    return fullfilleddata;
  };
  // [2019-09-25] end
  
  
  // 트리플용 센터통계 수정 [2019-11-01]
  const aggregateCenterData = map(data => {
    const [DT_KEY, PREV_RESOURCE_KEY] = split("|", data.label);
   
    const getDataConnectedAgent = pluck("CONNECTED_AGENT_IN");
    const getDataAcceptedAgent = pluck("ACCEPTED_AGENT");
    const getDataResponseWithin = pluck("RESPONSE_WITHIN_20S");
    const getDataAbandoned = pluck("ABANDONED");
  
    const sumDataConnectedAgent = sum(getDataConnectedAgent(data.result));
    const sumDataAcceptedAgent = sum(getDataAcceptedAgent(data.result));
    const sumDataResponseWithin = sum(getDataResponseWithin(data.result));
    const sumDataAbandoned = sum(getDataAbandoned(data.result));
  
    return {
      DT_KEY,
      PREV_RESOURCE_KEY,
      CONNECTED_AGENT_IN: sumDataConnectedAgent,
      ACCEPTED_AGENT: sumDataAcceptedAgent,
      RESPONSE_WITHIN_20S: sumDataResponseWithin,
      ABANDONED: sumDataAbandoned
    };
  });
  
  const summarizeServiceQueueCenterData = service_queue_data => new Promise((resolve, reject) => {
    resolve(
      pipe(
        groupByDTKeyAndPrevResourceKey,
        toPairs,
        splitArrayDataToLabelAndResult,
        aggregateCenterData
      )(service_queue_data)
    );
  });
  
  const addCenterStatsZeroValue = data => {
    return { ...data, 
      CONNECTED_AGENT_IN: data.CONNECTED_AGENT_IN || 0, 
      ACCEPTED_AGENT: data.ACCEPTED_AGENT || 0, 
      RESPONSE_WITHIN_20S: data.RESPONSE_WITHIN_20S || 0,
      ABANDONED: data.ABANDONED || 0,
      RESPONSE_RATE: data.RESPONSE_RATE || 0,  
    }
  }
  
  const addCenterStatsColumnZeroValueTo = data => new Promise((resolve, reject) => {
    resolve(map(addCenterStatsZeroValue, data));
  });
  // [2019-11-01] end
  
  
  // [2020-07-06] added for agent-states-infos ZIGZAG
  const onDtKeyAndAgentName = props(["DT_KEY", "AGENT_NAME"]);
  
  const joinOnDateTimeKeyAndAgentName = (left_table, right_table) => new Promise((resolve, reject) => {
    resolve(addMetaData(onDtKeyAndAgentName, onDtKeyAndAgentName)(left_table, right_table));
  });
  
  // const outerJoinOnDtKeyAndAgentName = (left_table, right_table) => new Promise((resolve, reject) => {
  //   resolve(joinOuter(onDtKeyAndAgentName, onDtKeyAndAgentName, left_table, right_table));
  // });
    
  
  // [2020-07-06] end
  
  module.exports = {
    onServiceResourceKey,
    leftJoinOnDIDResourceKey,
    leftJoinOnServiceResourceKey,
    leftJoinOnPrevResourceKeyAndDIDResourceKey,
    leftJoinOnDateTimeKey,
    addSummarizedServiceQueueData,
    addMetaData,
    addIvrProcessingColumnTo,
    addServiceConnectReqColumnZeroValueTo,
    addCenterStatsColumnZeroValueTo,
    summarizeServiceQueueData,
    mergeDIDAndServiceQueue,
    addDtKeyColumnTo,
    filterByEnterQueue,
    filterByConsultConnectQueue,
    filterByKeyNumber,
    filterByServiceName,
    filterTwoSlot,
    filterNone,
    sortReceiveCallInfos,
    appendCallbackItem,
    outerJoinOnDateTimeAndSvcResrcKey,
    outerJoinOnDTKeyAndDateTimeKey,
    fullfillServiceMisoData,
    summarizeServiceQueueCenterData,
    sortByAgentName,
    sortByAgentNameAndDate,
    joinOnDateTimeKeyAndAgentName
  };
  