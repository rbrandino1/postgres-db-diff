class PostgresRepository {
  constructor(postgresConnector) {
    this.postgresConnector = postgresConnector
  }

  selectSchemaInformationWithConstraints() {
    const queryText = `SELECT kcu.table_schema,
      kcu.table_name,
      tco.constraint_name,
      kcu.ordinal_position AS position,
      kcu.column_name AS key_column
       FROM information_schema.table_constraints tco
       JOIN information_schema.key_column_usage kcu 
         ON kcu.constraint_name = tco.constraint_name
         AND kcu.constraint_schema = tco.constraint_schema
         AND kcu.constraint_name = tco.constraint_name
       WHERE tco.constraint_type = 'PRIMARY KEY'
       ORDER BY kcu.table_schema,
         kcu.table_name,
         position`
    return this.postgresConnector.connectAndExecute(queryText)
  }

  selectAllRowsFromTable(schema) {
    const queryText = `SELECT * FROM ${schema.table_name} ORDER BY ${schema.constraints_fields.join(',')}`
    return this.postgresConnector.connectAndExecute(queryText)
  }

}

module.exports = PostgresRepository
