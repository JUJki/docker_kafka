const {processKryptonopolis} = require ('./app/kraken')
const {startConsumer} = require('./app/consumer-starter');

startConsumer('Kryptonopolis', processKryptonopolis);
