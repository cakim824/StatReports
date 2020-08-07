// var logger = require('../../../utils/logger')({ dirname: '', filename: '', sourcename: 'interaction-types.controller.js' });
var { sendPreparedStatementToInfomart } = require("../../../utils/mssql");

getInteractionTypeKeys = async(interaction_type) => {
    
    var query =
    `
    SELECT INTERACTION_TYPE_KEY
    FROM INTERACTION_TYPE
    WHERE INTERACTION_TYPE_CODE = '${interaction_type}'
    ;`
    ;

    console.log(`[get][getInteractionTypeKeys]: query = ` + query);
  
    const rows = await sendPreparedStatementToInfomart(query);

    var interactionType = rows;	
    interaction_type_keys =  ` (`	
    for(var i=0;  interactionType[i]; i++) {
      if(i==0) interaction_type_keys = interaction_type_keys + interactionType[i].INTERACTION_TYPE_KEY;	
      else interaction_type_keys = interaction_type_keys + `, ` + interactionType[i].INTERACTION_TYPE_KEY	
    }	 
    interaction_type_keys += `)` ;

    console.log("[interaction-types]" + interaction_type + " interaction_type_keys: " + interaction_type_keys);
    return interaction_type_keys;

};


module.exports = {
    getInteractionTypeKeys,
}