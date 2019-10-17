const fs = require('fs')
const PostgresConnector = require('./core/postgres-connector')
const PostgresRepository = require('./core/postgres-repository')

class Executor {
  constructor(container) {
    const connectorPostgresOfficial = new PostgresConnector(container.postgresOfficial)
    const connectorPostgresIntegration = new PostgresConnector(container.postgresIntegration)

    this.repositoryPostgresOfficial = new PostgresRepository(connectorPostgresOfficial)
    this.repositoryPostgresIntegration = new PostgresRepository(connectorPostgresIntegration)
    this.outputScriptFile = container.outputScriptFile
  }

  async execute() {
    const officialSchemaInformation = await this._getSchemaInformationWithConstraintsFromPostgresOfficial()

    if (officialSchemaInformation.length > 0) {
      for (const schemaItem of officialSchemaInformation) {
        await this._processSchemaItem(schemaItem)
      }
    }
  }

  async _getSchemaInformationWithConstraintsFromPostgresOfficial() {
    const result = await this.repositoryPostgresOfficial.selectSchemaInformationWithConstraints()

    return result.rows.reduce((acc, row) => {
      const accExistIndex = acc.findIndex(obj => obj.table_name === row.table_name && obj.constraint_name === row.constraint_name)
      if (accExistIndex === -1) {
        acc.push({
          'table_schema': row.table_schema,
          'table_name': row.table_name,
          'constraint_name': row.constraint_name,
          'constraints_fields': [row.key_column]
        })
      } else {
        acc[accExistIndex].constraints_fields.push(row.key_column)
      }

      return acc
    }, [])
  }

  async _processSchemaItem(schemaItem) {
    global.logger.info(schemaItem.table_name, { schemaItem })

    const currentSchemaOfficialValues = await this.repositoryPostgresOfficial.selectAllRowsFromTable(schemaItem)
    const currentSchemaIntegrationValues = await this.repositoryPostgresIntegration.selectAllRowsFromTable(schemaItem)

    // Integration table is empty
    if (!currentSchemaIntegrationValues.rows.length) {
      return
    }

    // Quando official.length está zerada e integration está populada -> Gera apenas Inserts
    if (!currentSchemaOfficialValues.rows.length) {
      this._outputFileWriteHeaderSchema(schemaItem.table_name)

      for (const rowItem of currentSchemaIntegrationValues.rows) {
        const queryText = this._generateSqlScriptInsert({
          schemaItem,
          rowItem
        })

        this._outputFileWriteNewLine(queryText)
      }

      return
    }

    // Quando existem dados em ambas e devemos fazer o diff -> Gera Inserts \ updates das diferenças
    const rowsShouldBeInserted = currentSchemaIntegrationValues.rows.filter(rowIntegration =>
      !currentSchemaOfficialValues.rows.some(rowOfficial => this._isObjectsEquivalent({
        objectA: this._pickSomePropertiesOfObject(rowIntegration, schemaItem.constraints_fields),
        objectB: this._pickSomePropertiesOfObject(rowOfficial, schemaItem.constraints_fields)
      })))

    const rowShouldBeUpdated = currentSchemaIntegrationValues.rows.filter(rowIntegration =>
      currentSchemaOfficialValues.rows.some(rowOfficial => this._isObjectsEquivalent({
        objectA: this._pickSomePropertiesOfObject(rowIntegration, schemaItem.constraints_fields),
        objectB: this._pickSomePropertiesOfObject(rowOfficial, schemaItem.constraints_fields)
      })))
      .filter(rowIntegration =>
        !currentSchemaOfficialValues.rows.some(rowOfficial => this._isObjectsEquivalent({
          objectA: this._omitSomePropertiesOfObject(rowIntegration, schemaItem.constraints_fields),
          objectB: this._omitSomePropertiesOfObject(rowOfficial, schemaItem.constraints_fields)
        })))

    if (rowsShouldBeInserted.length > 0) {
      this._outputFileWriteHeaderSchema(schemaItem.table_name)

      for (const rowItem of rowsShouldBeInserted) {
        const queryText = this._generateSqlScriptInsert({
          schemaItem,
          rowItem
        })

        this._outputFileWriteNewLine(queryText)
      }
    }

    if (rowShouldBeUpdated.length > 0) {
      this._outputFileWriteHeaderSchema(schemaItem.table_name)

      for (const rowItem of rowShouldBeUpdated) {
        const queryText = this._generateSqlScriptUpdate({
          schemaItem,
          rowItem
        })

        this._outputFileWriteNewLine(queryText)
      }
    }
  }

  _isObjectsEquivalent({ objectA, objectB }) {
    const aProps = Object.getOwnPropertyNames(objectA)
    const bProps = Object.getOwnPropertyNames(objectB)

    if (aProps.length !== bProps.length) {
      return false
    }

    for (let i = 0; i < aProps.length; i++) {
      const propName = aProps[i]

      if (objectA[propName] !== objectB[propName]) {
        return false
      }
    }

    return true
  }

  _generateSqlScriptInsert({ schemaItem, rowItem }) {
    const string_table_name = schemaItem.table_name

    const splittedAllColumnsAndValues = this._convertRowObjectToColumnsAndValuesArray({ rowItem })
    const string_all_columns = splittedAllColumnsAndValues.columns.join()
    const string_all_values = splittedAllColumnsAndValues.values
      .map(value => typeof value === 'string' ? `'${value}'` : (value === null || value === undefined ? 'NULL' : value))
      .join()

    return `INSERT INTO ${string_table_name} (${string_all_columns}) VALUES (${string_all_values})`
  }

  _generateSqlScriptUpdate({ schemaItem, rowItem }) {
    const string_table_name = schemaItem.table_name
    const splittedColumnsAndValuesWithoutConstraits = this._convertRowObjectToArrayWithoutConstraints({
      rowItem,
      constraintsFields: schemaItem.constraints_fields
    })
    const string_sets_without_constraints = splittedColumnsAndValuesWithoutConstraits.join()

    return `UPDATE ${string_table_name} SET ${string_sets_without_constraints};`
  }

  _generateSqlScriptInsertWithUpsert({ schemaItem, rowItem }) {
    const string_table_name = schemaItem.table_name
    const string_constraint_name = schemaItem.constraint_name

    const splittedAllColumnsAndValues = this._convertRowObjectToColumnsAndValuesArray({ rowItem })
    const string_all_columns = splittedAllColumnsAndValues.columns.join()
    const string_all_values = splittedAllColumnsAndValues.values
      .map(value => typeof value === 'string' ? `'${value}'` : (value === null || value === undefined ? 'NULL' : value))
      .join()

    const splittedColumnsAndValuesWithoutConstraits = this._convertRowObjectToArrayWithoutConstraints({
      rowItem,
      constraintsFields: schemaItem.constraints_fields
    })

    let queryText = `INSERT INTO ${string_table_name} (${string_all_columns}) VALUES (${string_all_values}) ON CONFLICT ON CONSTRAINT ${string_constraint_name}`

    const string_sets_without_constraints = splittedColumnsAndValuesWithoutConstraits.join()
    queryText += splittedColumnsAndValuesWithoutConstraits.length > 0 ? ` DO UPDATE SET ${string_sets_without_constraints};` : ` DO NOTHING;`

    return queryText
  }

  _convertRowObjectToColumnsAndValuesArray({ rowItem }) {
    return Object.entries(rowItem)
      .reduce((acc, [key, value]) => {
        acc.columns.push(key)
        acc.values.push(value)
        return acc
      }, {
        columns: [],
        values: []
      })
  }

  _convertRowObjectToArrayWithoutConstraints({ rowItem, constraintsFields }) {
    if (!Array.isArray(constraintsFields) || !constraintsFields.length) {
      return null
    }

    return Object.entries(rowItem)
      .reduce((acc, [key, value]) => {
        if (!constraintsFields.some(constraintField => constraintField === key)) {
          acc.push(`${key}=${typeof value === 'string' ? `'${value}'` : (value === null || value === undefined ? 'NULL' : value)}`)
        }

        return acc
      }, [])
  }

  _pickSomePropertiesOfObject(object, arrayProps) {
    return arrayProps.reduce((acc, prop) => ({
      ...acc,
      [prop]: object[prop]
    }), {})
  }

  _omitSomePropertiesOfObject(object, arrayProps) {
    return arrayProps.reduce((acc, prop) => {
      const { [prop]: ignored, ...rest } = acc
      return rest
    }, object)
  }

  _outputFileWriteNewLine(textLine) {
    fs.writeFileSync(this.outputScriptFile, `${textLine}\n`, { flag: 'a+' }, (err) => {
      global.logger.error('Error on write output file', { err })
    })
  }

  _outputFileWriteHeaderSchema(textLine) {
    this._outputFileWriteNewLine(`\n`)
    this._outputFileWriteNewLine(`/* ${'*'.repeat(50)} */`)
    this._outputFileWriteNewLine(`/* ${textLine}`)
    this._outputFileWriteNewLine(`/* ${'*'.repeat(50)} */`)
  }

}

module.exports = Executor
