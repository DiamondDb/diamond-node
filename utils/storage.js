const recordUtils = require('diamond-core').recordUtils
const { PAGE_SIZE } = require('./constants')

const makeFileMap = (operations, rootPath) => {
  return operations.reduce((map, op) => {
    const { table, record, id } = op.data
    const pageIdx = Math.floor(id/PAGE_SIZE)
    const fileName = `${rootPath}${table.name}.${pageIdx}.dat`
    const recordString = recordUtils.makeRecordString(table, record)
    map[fileName] = map[fileName] || []
    map[fileName][id] = recordString
    return map
  }, {})
}

const testFunctions = {
  EQ: (value, testValue) => value === testValue,
  LT: (value, testValue) => value < testValue
}

const makeFilterFunc = (key, comparator, testValue) => {
  const test = testFunctions[comparator]
  return (records, result) => {
    for(let i = 0; i < records.length; i++){
      const record = records[i]
      const value = record[key]
      if(test(value, testValue)){
        result.results.push(record)
      }
    }
  }
}

const parsePage = (table, rawPage) => {
  const regex = new RegExp('.{1,' + table.size + '}', 'g')
  const pageString = rawPage.toString()
  const recordStrings = pageString.match(regex)
  return recordStrings.map(recordString => {
    return recordUtils.parseRecord(recordString, table.schema)
  })
}

const makeIndexArray = (n) => {
  const arr = []
  for(let i = 0; i < n; i++){
    arr[i] = i
  }
  return arr
}

module.exports = {
  makeFileMap,
  makeFilterFunc,
  parsePage,
  makeIndexArray
}
