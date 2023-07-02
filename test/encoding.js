const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers/index.js')

test('basic encoding', async function (t) {
  const { db } = await create(t, { keyEncoding: 'binary', valueEncoding: 'json' })

  await db.put('/a', { num: 123 })
  t.alike(await db.get('/a'), { seq: 1, key: b4a.from('/a'), value: { num: 123 } })

  await db.put(b4a.from('/b'), { num: 321 })
  t.alike(await db.get('/b'), { seq: 2, key: b4a.from('/b'), value: { num: 321 } })
})
