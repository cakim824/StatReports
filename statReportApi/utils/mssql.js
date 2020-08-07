const mssql = require("mssql");
const config = require("../config/dbInfo.js");

const infomart_config = Object.assign(config.mssql, {
  pool: {
    max: 1000,
    idleTimeoutMillis: 10
  }
}) ;

const DATA_TYPES = {
  Bit: mssql.Bit,
  BigInt: mssql.BigInt,
  Decimal: mssql.Decimal,
  Float: mssql.Float,
  Int: mssql.Int,
  Money: mssql.Money,
  Numeric: mssql.Numeric,
  SmallInt: mssql.SmallInt,
  SmallMoney: mssql.SmallMoney,
  Real: mssql.Real,
  TinyInt: mssql.TinyInt,
  Char: mssql.Char,
  NChar: mssql.NChar,
  Text: mssql.Text,
  NText: mssql.NText,
  VarChar: mssql.VarChar,
  NVarChar: mssql.NVarChar,
  Xml: mssql.Xml,
  Time: mssql.Time,
  Date: mssql.Date,
  DateTime: mssql.DateTime,
  DateTime2: mssql.DateTime2,
  DateTimeOffset: mssql.DateTimeOffset,
  SmallDateTime: mssql.SmallDateTime,
  UniqueIdentifier: mssql.UniqueIdentifier,
  Variant: mssql.Variant,
  Binary: mssql.Binary,
  VarBinary: mssql.VarBinary,
  Image: mssql.Image,
  UDT: mssql.UDT,
  Geography: mssql.Geography,
  Geometry: mssql.Geometry
};

const getType = data => typeof data;
const getDataType = type => DATA_TYPES[type];

const getConnection = db_config => new mssql.ConnectionPool(db_config).connect();

const connectionClose = () => mssql.close();

const getPreparedStatement = db_connection =>
  new mssql.PreparedStatement(db_connection);

const executePreparedStatement = async (prepared_statement, query, parameters = {}) => {
  try {
    await prepared_statement.prepare(query);
    const { recordset } = await prepared_statement.execute(parameters);
    await prepared_statement.unprepare();
    return recordset;
  } catch (error) {
    throw error;
  }
};

const setParameterTypesIn = (prepared_statement, parameter_types) => {
  for (const key in parameter_types) {
    const data_type = getDataType(parameter_types[key]);
    prepared_statement.input(key, data_type);
  }
}

const sendPreparedStatementToInfomart = async (query, parameters, parameter_types) => {
  try { 
    const db_connection = await getConnection(infomart_config);
    const prepared_statement = getPreparedStatement(db_connection);

    setParameterTypesIn(prepared_statement, parameter_types);
    return await executePreparedStatement(prepared_statement, query, parameters);
  } catch (error) {
    throw error;
  }
};

// {2020-05-19 추가}
const sendPreparedQueryToInfomart = async (query, parameters) => {
  try { 
    const db_connection = await getConnection(infomart_config);
    const prepared_statement = getPreparedStatement(db_connection);

    return await executePreparedStatement(prepared_statement, query, parameters);
  } catch (error) {
    throw error;
  }
};

module.exports = {
  getConnection,
  getPreparedStatement,
  executePreparedStatement,
  connectionClose,
  sendPreparedStatementToInfomart,
  sendPreparedQueryToInfomart
};
