const router = require('express').Router();
const controller = require('./consults-history.controller');

router.get('/:date_unit', controller.read);

module.exports = router;
