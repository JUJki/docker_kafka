const {logger} = require('../app/utils/logger');

const fakeSendToKryptonopolis = model =>
  new Promise(resolve =>
    setTimeout(() => {
      logger.log('info', `dans send kryptonopolis ${JSON.stringify(model)}`);
      resolve(model);
    }, 5000)
  ).catch(error => {
    logger.log('error', `Unable to connect to kryptonopolis`);
    logger.log('debug', error);
  });

module.exports = {fakeSendToKryptonopolis};
