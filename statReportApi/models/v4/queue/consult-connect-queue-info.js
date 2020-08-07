/**
 * @author cakim@hansol.com
 */
const { sendPreparedStatementToPortalDB } = require('../../../utils/mariadb');
const { filterArgumentsIncludeKorean, filterDateArguments, filterArgumentsNumericList } = require('../../../utils/common');
// const logger = require("../../../utils/logger")({
//   dirname: "",
//   filename: "",
//   sourcename: "v4/consult-connect-queue-info.js"
// });

const findBySiteCd =  async ( { site_cd, end_date, endpoint_index_keys } ) => {
  try {
    if (!site_cd) {
      throw new Error('site_cd 파라미터에 값이 없습니다.')
    }

    // to prevent SQL Injection attack
    var filteredSiteCd = filterArgumentsIncludeKorean(site_cd);
    var filteredEndDate = filterDateArguments(end_date);
    var filteredEndPointIndexKeys = filterArgumentsNumericList(endpoint_index_keys);
    if ( filteredSiteCd != site_cd || filteredEndPointIndexKeys != endpoint_index_keys ) {
      console.log("[consult-connect-queue-info] arguments filtered: site_cd=" + site_cd + ", endpoint_index_keys=" + endpoint_index_keys);
    }

    var endpoint_index_map = ` AND A.MAPPING_INDEX IN (` + filteredEndPointIndexKeys + `)`;

    const query = `
    SELECT MAPPING_INDEX AS SERVICE_NAME_INDEX, ROUTING_POINT, VQ_KEY AS SERVICE_RESOURCE_KEY, SERVICE_NAME, ROUTING_POINT_TYPE, PREV_MAPPING_INDEX AS KEY_NUMBER_INDEX
            , (SELECT SERVICE_NAME FROM tb_rp_vq_mapping_log_detail WHERE MAPPING_INDEX = A.PREV_MAPPING_INDEX ) as KEY_NUMBER
            , (SELECT VQ_KEY FROM tb_rp_vq_mapping_log_detail WHERE MAPPING_INDEX = A.PREV_MAPPING_INDEX) as PREV_RESOURCE_KEY
    FROM tb_rp_vq_mapping_log_detail A
    WHERE SITE_CD = ?
    AND ROUTING_POINT_TYPE = '0002'
    AND APPLY_DATE= (SELECT MAX(APPLY_DATE) FROM tb_rp_vq_mapping_log WHERE APPLY_DATE <= ? AND SITE_CD = ?)
    ; `
    ;

    const params = [filteredSiteCd, filteredEndDate, filteredSiteCd];
    console.log("[consult-connect-queue-info] query: " + query + "\n    params: " + JSON.stringify(params));

    const rows = await sendPreparedStatementToPortalDB({ query, params });

    return rows;
  } catch (error) {
    throw error;
  }
};

module.exports = {
  findBySiteCd,
}