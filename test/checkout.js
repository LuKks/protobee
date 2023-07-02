const test = require('brittle')
const { create } = require('./helpers/index.js')

test('snapshot', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  t.is(db.version, 2)

  const snap = db.snapshot()
  t.is(snap.version, 2)

  await db.put('/a', '2')
  t.is(db.version, 3)
  t.is(snap.version, 2)

  t.alike(await snap.get('/a'), { seq: 1, key: '/a', value: '1' })
  t.is(snap.version, 2)

  await db.close()

  t.alike(await snap.get('/a'), { seq: 1, key: '/a', value: '1' })

  await snap.close()
})

test('checkout', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  await db.put('/a', '2')
  await db.put('/a', '3')

  const snap = db.checkout(3)
  t.is(snap.version, 3)

  t.alike(await snap.get('/a'), { seq: 2, key: '/a', value: '2' })

  await db.close()

  t.alike(await snap.get('/a'), { seq: 2, key: '/a', value: '2' })

  await snap.close()
})

test('future checkout', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')

  const snap = db.checkout(3)
  t.is(snap.version, 3)

  const get = snap.get('/a')

  t.ok(await db.get('/a'))
  await db.put('/a', '2')

  t.alike(await get, { seq: 2, key: '/a', value: '2' })

  await snap.close()
})
