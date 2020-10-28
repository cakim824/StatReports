const router = require('express').Router();

const receiveCallInfos = require('./receive-call-infos');
const serviceInfos = require('./service-infos');
const agentGroupOutputs = require('./agent-group-outputs');
const agentLoginInfos = require('./agent-login-infos');
const agentOutputs = require('./agent-outputs');
const agentTotalOutputs = require('./agent-total-outputs');
const agentStatesInfos = require('./agent-states-infos');
const callbacks = require('./callbacks');
const guideMessages = require('./guide-messages');
const consultsHistory = require('./consults-history');

router.use('/receive-call-infos', receiveCallInfos);
router.use('/service-infos', serviceInfos);
router.use('/agent-group-outputs', agentGroupOutputs);
router.use('/agent-outputs', agentOutputs);
router.use('/agent-total-outputs', agentTotalOutputs);
router.use('/agent-login-infos', agentLoginInfos);
router.use('/agent-states-infos', agentStatesInfos);
router.use('/callbacks', callbacks);
router.use('/guide-messages', guideMessages);
router.use('/consults-history', consultsHistory)

module.exports = router;