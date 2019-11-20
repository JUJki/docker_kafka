const {fakeSendToKryptonopolis} = require('./kryptonopolis');
const R = require('ramda');

const condNextStep_ = cond =>
    R.pipe(
        R.prop('to'),
        R.equals(cond)
    );

const callKryptonopolis = R.pipe(
    R.dissoc('to'),
    fakeSendToKryptonopolis
);

const processKryptonopolis = R.cond([
    [condNextStep_('train'), callKryptonopolis],
    [R.T, krypto => log('warn', `Krypto train not handled ${krypto}`)]
]);

module.exports = {processKryptonopolis};
