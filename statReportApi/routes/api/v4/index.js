const router = require('express').Router();
const reports = require('./reports');


router.use('/reports', reports);


module.exports = router;