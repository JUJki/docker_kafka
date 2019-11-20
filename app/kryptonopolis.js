const R = require('ramda');
const {log} = require('loggy-log');

const fakeSendToKryptonopolis = model => new Promise(resolve =>
    setTimeout(() => {
        console.log('dans send kryptonopolis');
        resolve(model)
    }, 5000))
    .catch(error => {
    log('error', `Unable to connect to kryptonopolis`);
    log('debug', error);
});

module.exports = {fakeSendToKryptonopolis};
