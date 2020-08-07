/**
 * @author cakim@hansol.com
 */
const { sendPreparedStatementToPortalDB } = require('../../../utils/mariadb');
const { filterArgumentsIncludeKorean, filterDateArguments, filterArgumentsNumericList } = require('../../../utils/common');
// const logger = require("../../../utils/logger")({
//   dirname: "",
//   filename: "",
//   sourcename: "v4/enter-queue-info.js"
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
    if ( filteredSiteCd != site_cd || filteredEndDate != end_date || filteredEndPointIndexKeys != endpoint_index_keys ) {
      console.log("[enter-queue-info] arguments filtered: site_cd=" + site_cd + ", end_date=" + end_date + ", endpoint_index_keys=" + endpoint_index_keys);
    }
  
    const endpoint_index_map = ` AND MAPPING_INDEX IN (` + filteredEndPointIndexKeys + `)`

    const query = `
    SELECT MAPPING_INDEX AS KEY_NUMBER_INDEX, ROUTING_POINT, VQ_KEY AS DID_RESOURCE_KEY, SERVICE_NAME AS KEY_NUMBER, ROUTING_POINT_TYPE
    FROM tb_rp_vq_mapping_log_detail
    WHERE SITE_CD = ?
    AND APPLY_DATE = (SELECT MAX(APPLY_DATE)
                 FROM tb_rp_vq_mapping_log
                 WHERE APPLY_DATE <= ?
                 AND   SITE_CD = ?)
    AND ROUTING_POINT_TYPE='0001'
    AND INFOMART_MODE = '00001'
    ; `
    ;

    const params = [filteredSiteCd, filteredEndDate, filteredSiteCd];
    console.log("[enter-queue-info] query: " + query + "\n    params: " + JSON.stringify(params));

    const rows = await sendPreparedStatementToPortalDB({ query, params });

    return rows;
  } catch (error) {
    throw error;
  }
};

module.exports = {
  findBySiteCd,
}