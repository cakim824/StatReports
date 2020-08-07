const router = require("express").Router();
const controller = require("./callbacks.controller");

router.get('/:date_unit', controller.read);

module.exports = router;
