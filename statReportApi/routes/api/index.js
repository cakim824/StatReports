const router = require('express').Router();

router.use('/v4', require('./v4'));

module.exports = router;