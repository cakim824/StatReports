const router = require('express').Router();
const controller = require('./service-infos.controller');

router.get('/:date_unit', controller.read);

module.exports = router;
