const test = require('brittle')
const Protobee = require('./index.js')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const createTestnet = require('hyperdht/testnet')
const RAM = require('random-access-memory')
const c = require('compact-encoding')

test('get and put', async function (t) {
  const { db } = await create(t)

  t.is(await db.get('/a'), null)
  await db.put('/a', '1')
  t.alike(await db.get('/a'), { seq: 1, key: '/a', value: '1' })

  t.is(await db.get('/b'), null)
  await db.put('/b')
  t.alike(await db.get('/b'), { seq: 2, key: '/b', value: null })
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

test('snapshot', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  t.is(db.version, 2)

  const snap = db.snapshot()
  t.is(snap.version, 2)

  await db.put('/a', '2')
  t.is(db.version, 3)

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

test('batch', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')

  const batch = db.batch()
  t.is(batch.version, 2)

  await batch.lock()

  const put = db.put('/b', '2')

  t.is(await batch.get('/b'), null)
  t.is(batch.version, 2)

  await batch.put('/b', '3')
  t.is(batch.version, 3)
  t.alike(await batch.get('/b'), { seq: 2, key: '/b', value: '3' })

  t.is(db.version, 2)
  await batch.flush()
  t.is(db.version, 3)

  await put
  t.is(db.version, 4)
  t.alike(await db.get('/b'), { seq: 3, key: '/b', value: '2' })
})

test('batch flush vs close', async function (t) {
  const { db } = await create(t)

  const a = db.batch()
  await a.put('/a', '1')
  await a.flush()

  t.alike(await db.get('/a'), { seq: 1, key: '/a', value: '1' })

  const b = db.batch()
  await b.put('/b', '2')
  await b.close()

  t.alike(await db.get('/b'), null)

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

test('basic peek', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')

  t.alike(await db.peek({ gt: '/a' }), { seq: 2, key: '/b', value: '2' })
  t.alike(await db.peek({ reverse: true }), { seq: 2, key: '/b', value: '2' })
  t.alike(await db.peek({ reverse: true }), { seq: 2, key: '/b', value: '2' })
})

test('multiple writer clients', async function (t) {
  const { db } = await create(t)

  const db1 = new Protobee(db.core.keyPair, { dht: db.dht })
  t.is(db1.version, 1)
  await db1.ready()
  t.is(db1.version, 1)
  await db1.put('/a', '1')
  t.is(db1.version, 2)

  const db2 = new Protobee(db.core.keyPair, { dht: db.dht })
  t.is(db2.version, 1)
  await db2.ready()
  t.is(db2.version, 2)
  await db2.put('/b', '2')
  t.is(db2.version, 3)

  const db3 = new Protobee(db.core.keyPair, { dht: db.dht })
  t.is(db3.version, 1)
  await db3.ready()
  t.is(db3.version, 3)
  await db3.put('/c', '3')
  t.is(db3.version, 4)

  await db1.close()
  await db2.close()
  await db3.close()
})

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

async function create (t, opts = {}) {
  const testnet = await createTestnet()
  const bootstrap = testnet.bootstrap
  t.teardown(() => testnet.destroy(), { order: Infinity })

  const core = new Hypercore(RAM)
  const bee = new Hyperbee(core, { keyEncoding: c.any, valueEncoding: c.any }) // TODO: fix this (cas, etc)

  const server = new Protobee(bee, { bootstrap })
  t.teardown(() => server.close())

  await core.ready()
  if (opts.data) await bee.put('/test', 'abc')

  const db = new Protobee(core.keyPair, { bootstrap })
  t.teardown(() => db.close())

  await server.ready()
  await db.ready()

  return { server, db }
}
