const winston = require('winston');
const config = require('config');

const transport = () => new winston.transports.Console();

const localFormat = () => {
  const {align, colorize, combine, printf, timestamp} = winston.format;
  return combine(
    colorize(),
    align(),
    timestamp({format: 'YYYY-MM-DD hh:mm:ss SSS'}),
    printf(info => `${info.timestamp} [${info.level}]: ${info.message}`)
  );
};

const prodFormat = winston.format.json;

const format = config.get('env') === 'local' ? localFormat : prodFormat;

const logger = winston.createLogger({
  level: 'info',
  format: format(),
  transports: [transport()]
});

module.exports = {logger};
