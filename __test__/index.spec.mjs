import test from 'ava'

import { fromParquetBuffer } from '../index.js'
import {readFileSync} from 'fs'

test('Read parquet buffer', (t) => {
  const buf = readFileSync('./__test__/data.parquet')

  t.notThrows(() => {
    const ipc = fromParquetBuffer(buf, [])
  })
})
