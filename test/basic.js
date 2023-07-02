const test = require('brittle')
const { create } = require('./helpers/index.js')

test('get, put, del', async function (t) {
  const { db } = await create(t)

  t.is(await db.get('/a'), null)
  await db.put('/a', '1')
  t.alike(await db.get('/a'), { seq: 1, key: '/a', value: '1' })

  t.is(await db.get('/b'), null)
  await db.put('/b')
  t.alike(await db.get('/b'), { seq: 2, key: '/b', value: null })

  await db.del('/b')
  t.is(await db.get('/b'), null)
})

test('version', async function (t) {
  const { server, db } = await create(t)

  t.is(db.core.length, 0)
  t.is(db.version, 1)
  await db.put('/a', '1')
  t.is(db.core.length, 2)
  t.is(db.version, 2)

  await server.bee.put('/b', '2')
  t.is(db.core.length, 2)
  t.is(db.version, 2)
  await new Promise(resolve => setTimeout(resolve, 50))
  t.is(db.core.length, 3)
  t.is(db.version, 3)
})

test('update', async function (t) {
  const { server, db } = await create(t)

  t.is(db.version, 1)
  await server.bee.put('/b', '2')
  t.is(db.version, 1)

  await db.update()
  t.is(db.version, 2)
})

test('initial update', async function (t) {
  const { db } = await create(t, { data: true })
  t.is(db.version, 2)
})

test('put cas', async function (t) {
  const { db } = await create(t)

  t.is(db.version, 1)

  await db.put('/a', '1')
  t.is(db.version, 2)

  await db.put('/a', '1')
  t.is(db.version, 3)

  await db.put('/a', '1', { cas: true })
  t.is(db.version, 3)

  await db.put('/a', '2', { cas: true })
  t.is(db.version, 4)
})

test('peek', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')

  t.alike(await db.peek({ gt: '/a' }), { seq: 2, key: '/b', value: '2' })
  t.alike(await db.peek({ reverse: true }), { seq: 2, key: '/b', value: '2' })
  t.alike(await db.peek({ reverse: true }), { seq: 2, key: '/b', value: '2' })
})
