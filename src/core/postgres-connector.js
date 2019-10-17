const { Client, types } = require('pg')
const Moment = require('moment')

const DATATYPE_STRING = 1043
const DATATYPE_FLOAT = 1700
const DATATYPE_DATE = 1082
const DATATYPE_TIMESTAMP = 1114

// Override Type Parses
types.setTypeParser(DATATYPE_STRING, function(val) {
  return val.replace(new RegExp(`'`, `g`), `''`)
})

types.setTypeParser(DATATYPE_FLOAT, function(val) {
  return parseFloat(val)
})

types.setTypeParser(DATATYPE_DATE, function(val) {
  return val === null ? null : Moment(val)
    .format('YYYY-MM-DD')
})

types.setTypeParser(DATATYPE_TIMESTAMP, function(val) {
  return val === null ? null : Moment(val)
    .format('YYYY-MM-DD HH:mm:ss')
})

class PostgresConnector {
  constructor(connectionString) {
    this.connectionString = connectionString
  }

  async connectAndExecute(block) {
    const client = new Client({ connectionString: this.connectionString })
    await client.connect()

    try {
      return await client.query(block)
    } finally {
      await client.end()
    }
  }
}

module.exports = PostgresConnector
