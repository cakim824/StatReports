require('dotenv').config();
const mariadb = require("mysql2/promise");
const config = require("../config/dbInfo.js");
const c_cloud_config = config.mariaDB_APP;

// dbConfig Sample
// const c_cloud_config = {
//   host: "localhost",
//   port: "3306",
//   user: "root",
//   password: "1111",
//   database: "c_cloud"
// };

// query Sample
// const query = "SELECT * FROM tb_agent WHERE SITE_CD = ?";

// query parameter Sample
// const params = ["Dev"];

const getConnection = db_config => mariadb.createConnection(db_config);

const sendPreparedStatementTo = db_config => async ({ query, params = [] }) => {
  console.log("query: " + query);
  console.log("params: " + JSON.stringify(params));
  let connection = '';
  try {
    connection = await getConnection(db_config);
    const [rows] = await connection.execute(query, params);
    return rows;
  } catch (error) {
    throw error;
  } finally {
    connection && connection.end();
  }
};

const sendPreparedStatementToPortalDB = sendPreparedStatementTo(c_cloud_config);

module.exports = {
  getConnection,
  sendPreparedStatementToPortalDB
};
