const fs = require('fs')
const core = require('diamond-core')
const diskUtils = require('./utils/disk')
const constants = require('./utils/constants')
const storageUtils = require('./utils/storage')
const recordUtils = core.recordUtils
const tableUtils = core.tableUtils
const {
  PERSIST_ALL, INITIALIZE_PERSISTANCE,
  MAKE_TABLE, UPDATE_META,
  FETCH_RECORD, STORE_RECORD, FILTER_RECORDS,
  success, failure
} = core.operations.internal
const { READ, APPEND, PAGE_SIZE } = constants

const readFile = core.promisify(fs.readFile)
const makeFileName = (path, name, idx) => `${path}${name}.${idx}.dat`

module.exports = class Store {
  constructor(location) {
    this.root = location || './data/'
    this.metaFilePath = `${this.root}meta.txt`
    this.operations = []
    this.latestMetaUpdate = null
  }
  init(){
    return diskUtils.openOrCreate(this.metaFilePath, READ)
      .then(this._loadMeta.bind(this))
      .then((tables) => success(tables))
  }
  /* called by init() */
  _loadMeta(data) {
    return readFile(this.metaFilePath).then(data => {
      const tableData = data.toString()
      if(tableData.length){
        return tableUtils.parseTableString(tableData)
      }
    })
  }
  _clearOperations() {
    const operations = this.operations.slice()
    this.operations = []
    return operations
  }
  updateMeta() {
    const tables = this.latestMetaUpdate && this.latestMetaUpdate.tables
    if(tables){
      let meta = ''
      Object.keys(tables).forEach(tableName => {
        meta += tableUtils.makeTableString(tables[tableName])
      })
      return diskUtils.create(this.metaFilePath, meta)
    } else {
      return Promise.resolve()
    }
  }
  makeTable({ tableData }) {
    if(!tableData){
      return Promise.reject(failure('Create table message did not contain new table'))
    }
    const tableString = tableUtils.makeTableString(tableData)
    return diskUtils.append(this.metaFilePath, tableString).then(() => {
      return success()
    })
  }
  /* called by persist */
  _save(fileName, records) {
    return diskUtils.openOrCreate(fileName, APPEND).then(() => {
      return diskUtils.append(fileName, records)
    })
  }
  fetch({ table, id }){
    const schemaLength = table.size
    const pageIdx = Math.floor(id/PAGE_SIZE)
    const recordIdx = id % PAGE_SIZE
    const fileName = makeFileName(this.root, table.name, pageIdx)
    return readFile(fileName)
      .then(result => {
        const page = result.toString()
        const position = recordIdx * schemaLength
        const recordString = page.substring(position, position+schemaLength)
        const record = recordUtils.parseRecord(recordString, table.schema)
        record._id = recordIdx
        return success(record)
      })
      .catch(e => failure(e))
  }
  filter({ table, query: { key, comparator, value } }, resolve, reject){
    const numRecords = table.index-1
    const numPages = Math.ceil(numRecords/PAGE_SIZE)
    const indices = storageUtils.makeIndexArray(numPages)
    let filterFunc = storageUtils.makeFilterFunc(key, comparator, value)
    let retries = {}, result = { results: [] }
    let hasError = false
    while(indices.length && !hasError){
      const pageNumber = indices.pop()
      const fileName = makeFileName(this.root, table.name, pageNumber)
      readFile(fileName)
        .then(rawPage => {
          const records = storageUtils.parsePage(table, rawPage)
          filterFunc(records, result)
        })
        .catch(e => {
          if(retries[pageNumber] > 2){
            reject(`Page read error on page #${pageNumber}`)
            hasError = true
          } else {
            retries[pageNumber] = retries[pageNumber] || 0
            retries[pageNumber]++
            indices.push(pageNumber)
          }
        })
    }
    if(!hasError) resolve(result)
  }
  persist(){
    const operations = this._clearOperations()
    const storeOperations = operations.filter(msg => msg.operation === STORE_RECORD)
    const fileMap = storageUtils.makeFileMap(storeOperations, this.root)
    const promises = Object.keys(fileMap).map(fileName => {
      const recordString = fileMap[fileName].join('')
      return this._save(fileName, recordString)
    })
    if(promises.length){
      return this.updateMeta()
      .then(() => Promise.all(promises))
      .then(() => success())
      .catch(e => failure(e))
    } else {
      return success()
    }
  }
  message(message){
    console.log('store message: ', message.operation)
    switch(message.operation){
      case UPDATE_META:
        this.latestMetaUpdate = message.data
        return Promise.resolve()
      case MAKE_TABLE:
        return this.makeTable(message.data)
      case STORE_RECORD:
        this.operations.push(message)
        return Promise.resolve()
      case FETCH_RECORD:
        return this.fetch(message.data)
      case FILTER_RECORDS:
        return new Promise((resolve, reject) => {
          this.filter(message.data, resolve, reject)
        })
      case INITIALIZE_PERSISTANCE:
        return this.init()
      case PERSIST_ALL:
        return this.persist()
    }
  }
}
