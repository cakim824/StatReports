const router = require('express').Router();
const controller = require('./agent-login-infos.controller');

router.get('/daily/:id', controller.readDaily);

module.exports = router;