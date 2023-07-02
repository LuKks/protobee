const test = require('brittle')
const { create } = require('./helpers/index.js')

test('history stream', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')
  await db.del('/a')
  await db.put('/c', '3')

  const stream = db.createHistoryStream()

  t.is(stream.destroyed, false)

  const expected = [
    { type: 'put', seq: 1, key: '/a', value: '1' },
    { type: 'put', seq: 2, key: '/b', value: '2' },
    { type: 'del', seq: 3, key: '/a', value: null },
    { type: 'put', seq: 4, key: '/c', value: '3' }
  ]

  for await (const entry of stream) {
    t.alike(entry, expected.shift())
  }

  t.is(stream.destroyed, true)
})

test('history stream with options', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')
  await db.del('/a')
  await db.put('/c', '3')

  const stream = db.createHistoryStream({ gt: 1, lt: 4, reverse: true })

  const expected = [
    { type: 'del', seq: 3, key: '/a', value: null },
    { type: 'put', seq: 2, key: '/b', value: '2' }
  ]

  for await (const entry of stream) {
    t.alike(entry, expected.shift())
  }
})
