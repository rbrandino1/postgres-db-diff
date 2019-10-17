const dotenv = require('dotenv')
const winston = require('winston')

const Executor = require('./executor')

dotenv.config()

global.logger = winston.createLogger({
  transports: [new winston.transports.Console()]
})

const executor = new Executor({
  postgresOfficial: process.env.DATABASE_OFFICIAL_URL,
  postgresIntegration: process.env.DATABASE_INTEGRATION_URL,
  outputScriptFile: process.env.OUTPUT_SCRIPT_FILE
})

executor.execute()

//require('make-runnable');
