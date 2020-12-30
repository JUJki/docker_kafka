const R = require('ramda');
const {fakeSendToKryptonopolis} = require('./kryptonopolis');
const {fakeSendToElasticSearch} = require('./elastic-search');
const {fakeSendToHelheim} = require('./helheim');

const condNextStep_ = cond => R.pipe(R.prop('to'), R.equals(cond));

const delayInspector_ = igResponse =>
  new Promise(resolve => setTimeout(() => resolve(igResponse), 5000));

const callKryptonopolis_ = R.pipe(R.dissoc('to'), fakeSendToKryptonopolis);

const callElasticSearch_ = R.pipe(
  R.dissoc('to'),
  fakeSendToElasticSearch,
  R.then(delayInspector_),
  R.then(R.tap(igResponse => console.log('ff', igResponse))),
  R.then(R.applySpec({modelsInspectorGadget: R.prop('dbName')})),
  R.then(R.tap(dataHelheim => console.log('ffhelheim', dataHelheim))),
  R.then(R.tap(console.log)),
  R.then(oooo => oooo)
);

const callHelheim_ = R.pipe(
  R.dissoc('to'),
  fakeSendToHelheim,
  R.then(R.tap(console.log))
);

const processKryptonopolis = R.cond([
  [condNextStep_('train'), callKryptonopolis_],
  [condNextStep_('simple'), callKryptonopolis_],
  [R.T, krypto => console.log('warn', `Krypto train not handled ${krypto}`)]
]);

const processElasticSearch = R.cond([
  [condNextStep_('train'), callElasticSearch_],
  [R.T, krypto => console.log('warn', `Krypto train not handled ${krypto}`)]
]);

const processHelheim = R.cond([
  [condNextStep_(''), callHelheim_],
  [R.T, krypto => console.log('warn', `Helheim not handled ${krypto}`)]
]);

module.exports = {processKryptonopolis, processElasticSearch, processHelheim};
