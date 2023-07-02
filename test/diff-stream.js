const test = require('brittle')
const { create } = require('./helpers/index.js')

test('diff stream', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  const otherVersion = db.version
  await db.put('/b', '2')

  const stream = db.createDiffStream(otherVersion)

  t.is(stream.destroyed, false)

  const expected = [
    { left: { seq: 2, key: '/b', value: '2' }, right: null }
  ]

  for await (const diff of stream) {
    t.alike(diff, expected.shift())
  }

  t.is(stream.destroyed, true)
})

test('diff stream with passed snap', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  const snap = db.snapshot()
  await db.put('/b', '2')

  const stream = db.createDiffStream(snap)

  const expected = [
    { left: { seq: 2, key: '/b', value: '2' }, right: null }
  ]

  for await (const diff of stream) {
    t.alike(diff, expected.shift())
  }

  await snap.close()
})

test('diff stream with older snap as base', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  const snap = db.snapshot()
  await db.put('/b', '2')

  const stream = snap.createDiffStream(db)

  const expected = [
    { left: null, right: { seq: 2, key: '/b', value: '2' } }
  ]

  for await (const diff of stream) {
    t.alike(diff, expected.shift())
  }

  await snap.close()
})

test('diff stream with range', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')
  await db.put('/c', '3')

  const stream = db.createDiffStream(1, { gt: '/b' })

  const expected = [
    { left: { seq: 3, key: '/c', value: '3' }, right: null }
  ]

  for await (const diff of stream) {
    t.alike(diff, expected.shift())
  }
})
