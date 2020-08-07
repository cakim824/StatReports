const { sendPreparedStatementToPortalDB } = require('../../../utils/mariadb');
// var logger = require('../../../utils/logger')({
//   dirname: '',
//   filename: '',
//   sourcename: 'callbacks.js'
// });

var isNotEmpty = value => value != "";

const DATE_UNITS = {
    hourly: {
        main_view_name: "tb_callback_hour"
    },
    daily: {
        main_view_name: "tb_callback_day"
    },
    monthly: {
        main_view_name: "tb_callback_month"
    }
};

const getViewName = date_unit => DATE_UNITS[date_unit].main_view_name;

const getCallbackDatas = async ({ date_unit, site_cd, start_date, end_date, in_type, in_detail_type, dnis_index, endpoint_index, start_time, end_time }) => {

    const main_view_name = getViewName(date_unit);

    var madatoryParamList = ['site_cd', 'start_date', 'end_date'];
    var missingParamList = [];
    var existMandatoryParam = true;
  
    if (!site_cd) {
        existMandatoryParam = false;
        missingParamList.push('site_cd');
    }
    if (!start_date) {
        existMandatoryParam = false;
        missingParamList.push('start_date');
    }
    if (!end_date) {
        existMandatoryParam = false;
        missingParamList.push('end_date');
    }
  
    if (!existMandatoryParam) {
        var resData = {
            success: false,
            message: `필수 파라미터[${madatoryParamList.join(',')}] 중  [ ${missingParamList.join(',')}] 가 누락되었습니다.`
        };
        res.status(400).send(resData);
        return;
    }

    var in_type_query = "";
    var in_detail_type_query = "";
    var rp_vq_map_info_query = "";
    var time_range_query = "";

    if (isNotEmpty(in_type)) {
        in_type_query = `AND  t2.in_type = '${in_type}'`;
    }
    if (isNotEmpty(in_detail_type)) {
        in_detail_type_query = `AND  t2.in_detail_type = '${in_detail_type}'`;
    }
    if (isNotEmpty(dnis_index)) {
        rp_vq_map_info_query = `AND  t2.dnis = '${dnis_index}'`;
    }
    if (isNotEmpty(endpoint_index)) {
        rp_vq_map_info_query = `AND  t2.endpoint_dn = '${endpoint_index}' AND t2.dnis = '${dnis_index}'`;
    }
    if (isNotEmpty(start_time)) {
    // if ( start_time != "" ) {
        time_range_query = `  AND X.TIME_KEY BETWEEN '${start_time}' AND '${end_time}'`;
    }
    // var parameters = { date_unit, site_cd, start_date, end_date, in_type, in_detail_type, dnis_index, endpoint_index };
  
    var query =
    `
    SELECT *
    FROM (
    SELECT t2.row_date
       , LEFT(t2.row_date, 8) AS date_key
       , RIGHT(t2.row_date, 4) AS time_key
       , t2.site_cd
       , t2.in_type
       , t1.COM_SNM AS in_type_nm
       , t2.in_detail_type
       , t3.COM_SNM AS in_detail_type_nm
       , t2.in_count
       , t2.finish_count
       , t2.average_process_time
       , t2.average_process_try_count

       , (SELECT a.SERVICE_NAME 
        FROM tb_rp_vq_mapping_log_detail a, tb_rp_vq_mapping_log_detail b
        WHERE a.PREV_MAPPING_INDEX = b.MAPPING_INDEX 
        AND a.ROUTING_POINT = t2.endpoint_dn
        AND b.ROUTING_POINT = t2.dnis
        AND a.INFOMART_MODE = '00001'
        AND b.INFOMART_MODE = '00001'
        AND a.SITE_CD = '${site_cd}'
        AND a.APPLY_DATE = (SELECT MAX(APPLY_DATE) FROM tb_rp_vq_mapping_log_detail WHERE APPLY_DATE <= '${end_date}' AND SITE_CD = '${site_cd}')) AS endpoint_dn
  
        , (SELECT b.SERVICE_NAME 
          FROM tb_rp_vq_mapping_log_detail a, tb_rp_vq_mapping_log_detail b
          WHERE a.PREV_MAPPING_INDEX = b.MAPPING_INDEX 
          AND a.ROUTING_POINT = t2.endpoint_dn
          AND b.ROUTING_POINT = t2.dnis
          AND a.INFOMART_MODE = '00001'
          AND b.INFOMART_MODE = '00001'
          AND a.SITE_CD = '${site_cd}'
          AND a.APPLY_DATE= (SELECT MAX(APPLY_DATE) FROM tb_rp_vq_mapping_log_detail WHERE APPLY_DATE <= '${end_date}' AND SITE_CD = '${site_cd}')) AS dnis

    FROM   (
           SELECT COM_SCD
                , COM_SNM
           FROM tb_scode
           WHERE COM_LCD = 'IT000'
           AND USE_YN = 'Y'
         ) t1
         RIGHT OUTER JOIN ${main_view_name} t2 ON t1.COM_SCD = t2.in_type
         LEFT OUTER JOIN
         (
           SELECT COM_SCD
                , COM_SNM
           FROM tb_scode
           WHERE COM_LCD = 'IT03'
           AND USE_YN = 'Y'
         ) t3 ON t2.in_detail_type = t3.COM_SCD
    WHERE  row_date BETWEEN '${start_date}' AND '${end_date}'
    AND    site_cd = '${site_cd}'
    ${in_type_query}
    ${in_detail_type_query}
    ${rp_vq_map_info_query}
    ) X
    WHERE 1=1 
    ${time_range_query}
    ;
    `;
    
    const params = [ date_unit, site_cd, start_date, end_date, in_type, in_detail_type, dnis_index, endpoint_index, start_time, end_time ];
    const rows = await sendPreparedStatementToPortalDB({ query, params });
    console.log("[callbacks.controller][get]: query = " + query);

    return rows;
    
};


module.exports = {
    getCallbackDatas
};