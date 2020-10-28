// var logger = require('../../../utils/logger')({ dirname: '', filename: '', sourcename: 'interaction-types.controller.js' });
const { sendPreparedStatementToPortalDB } = require('../../../utils/mariadb');

getTenantKey = async(site_cd) => {
    
    // var query =
    // `
    // SELECT TENANT_KEY
    // FROM TENANT
    // WHERE TENANT_NAME = 'Environment'
    // ;`
    // ;
  
    // const rows = await sendPreparedStatementToInfomart(query);

    var query =
    `
    SELECT TENANT_KEY
    FROM tb_site
    WHERE SITE_CD = '${site_cd}'
    ;`
    ;
  
    const rows = await sendPreparedStatementToPortalDB({query});

    var tenant_key = rows[0].TENANT_KEY
    console.log(`[get][tenant_key]: ` + tenant_key);
    return tenant_key;

};


module.exports = {
    getTenantKey,
}