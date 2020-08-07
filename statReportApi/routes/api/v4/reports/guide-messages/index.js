const router = require("express").Router();
const controller = require("./guide-messages.controller");

router.get('/:date_unit', controller.read);

module.exports = router;
