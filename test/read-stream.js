const test = require('brittle')
const { create } = require('./helpers/index.js')

test('read stream', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')
  await db.put('/c', '3')

  const stream = db.createReadStream()

  t.is(stream.destroyed, false)

  const expected = [
    { seq: 1, key: '/a', value: '1' },
    { seq: 2, key: '/b', value: '2' },
    { seq: 3, key: '/c', value: '3' }
  ]

  for await (const entry of stream) {
    t.alike(entry, expected.shift())
  }

  t.is(stream.destroyed, true)
})

test('read stream with range', async function (t) {
  const { db } = await create(t)

  await db.put('a', '1')
  await db.put('b', '2')
  await db.put('c', '3')

  const stream = db.createReadStream({ gt: 'b' })

  const expected = [
    { seq: 3, key: 'c', value: '3' }
  ]

  for await (const entry of stream) {
    t.alike(entry, expected.shift())
  }
})

test('read stream out of range', async function (t) {
  const { db } = await create(t)

  await db.put('a', '1')
  await db.put('b', '2')
  await db.put('c', '3')

  const stream = db.createReadStream({ gt: 'c' })

  for await (const entry of stream) {
    t.fail('Should not read any entry: ' + entry.key)
  }

  t.pass()
})

test('stream without ready and with existing data', async function (t) {
  const { db } = await create(t, { data: true, ready: false })

  const actual = []

  for await (const entry of db.createReadStream({ keyEncoding: 'utf-8', valueEncoding: 'utf-8' })) {
    actual.push(entry.key)
  }

  t.alike(actual, ['/test'])

  await db.close()
})
