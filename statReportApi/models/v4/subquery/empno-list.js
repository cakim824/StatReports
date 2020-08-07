// var logger = require('../../../utils/logger')({ dirname: '', filename: '', sourcename: 'empno-list.js' });
var { sendPreparedStatementToPortalDB } = require("../../../utils/mariadb");

getEmpnoList = async({ agent_group, site_cd }) => {

    var query =
    `
    SELECT CONCAT("\'", EMP_NO, "\'") AS EMP_NO FROM tb_agent WHERE GROUP_CD = '${agent_group}' AND SITE_CD = '${site_cd}'
    ;`
    ;

    console.log(`[get][getEmpnoList]: query = ` + query);
    
    const params = [];
    const rows = await sendPreparedStatementToPortalDB({query, params});

    var empno = rows;	

    empno_list =  ` (`	
    for(var i=0;  empno[i]; i++) {
      if(i==0) empno_list = empno_list + empno[i].EMP_NO;	
      else empno_list = empno_list + `, ` + empno[i].EMP_NO;	
    }	 
    empno_list += `)` ;

    console.log("[agent_group]" + agent_group + " empno_list: " + empno_list);
    return empno_list;

};


module.exports = {
    getEmpnoList,
}