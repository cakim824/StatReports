const router = require("express").Router();
const controller = require("./agent-group-outputs.controller");


router.get('/:date_unit', controller.read);


module.exports = router;
