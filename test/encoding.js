const test = require('brittle')
const b4a = require('b4a')
const SubEncoder = require('sub-encoder')
const { create, collect } = require('./helpers/index.js')
const Protobee = require('../index.js')

test('basic encoding', async function (t) {
  const { db } = await create(t, { keyEncoding: 'binary', valueEncoding: 'json' })

  await db.put('/a', { num: 123 })
  t.alike(await db.get('/a'), { seq: 1, key: b4a.from('/a'), value: { num: 123 } })

  await db.put(b4a.from('/b'), { num: 321 })
  t.alike(await db.get('/b'), { seq: 2, key: b4a.from('/b'), value: { num: 321 } })
})

test('key encoding', async function (t) {
  const { db } = await create(t, { keyEncoding: 'binary', valueEncoding: 'json' })

  const keyEncoding = new SubEncoder('files', 'utf-8')

  await db.put('/a', '1', { keyEncoding })
  await db.put('/b', '2', { keyEncoding })

  t.is(await db.get('/a'), null)
  t.alike(await db.get('/a', { keyEncoding }), { seq: 1, key: '/a', value: '1' })

  await db.del('/a', { keyEncoding })

  t.is(await db.get('/a', { keyEncoding }), null)

  t.alike(await db.peek({ gt: '/a', keyEncoding }), { seq: 2, key: '/b', value: '2' })
  t.alike(await db.peek({ gt: '/a' }, { keyEncoding }), { seq: 2, key: '/b', value: '2' })

  t.alike(await collect(db.createReadStream({ keyEncoding })), [{ seq: 2, key: '/b', value: '2' }])
  t.alike(await collect(db.createReadStream({ gt: '/a' }, { keyEncoding })), [{ seq: 2, key: '/b', value: '2' }])

  t.alike(await collect(db.createHistoryStream({ keyEncoding })), [{ type: 'put', seq: 1, key: '/a', value: '1' }, { type: 'put', seq: 2, key: '/b', value: '2' }, { type: 'del', seq: 3, key: '/a', value: null }])
  t.alike(await collect(db.createHistoryStream({ gt: 2, keyEncoding })), [{ type: 'del', seq: 3, key: '/a', value: null }])

  t.alike(await collect(db.createDiffStream(2, { keyEncoding })), [{ left: null, right: { seq: 1, key: '/a', value: '1' } }, { left: { seq: 2, key: '/b', value: '2' }, right: null }])
  t.alike(await collect(db.createDiffStream(2, { gt: '/a', keyEncoding })), [{ left: { seq: 2, key: '/b', value: '2' }, right: null }])
})

test('key encoding for checkout and snapshot', async function (t) {
  const { db } = await create(t, { keyEncoding: 'binary', valueEncoding: 'json' })

  const keyEncoding = new SubEncoder('files', 'utf-8')

  await db.put('/a', '1', { keyEncoding })

  const snap1 = db.checkout(db.version, { keyEncoding })
  t.alike(await snap1.get('/a'), { seq: 1, key: '/a', value: '1' })
  await snap1.close()

  const snap2 = db.snapshot({ keyEncoding })
  t.alike(await snap2.get('/a'), { seq: 1, key: '/a', value: '1' })
  await snap2.close()
})

test('value encoding', async function (t) {
  const { db } = await create(t, { keyEncoding: 'binary', valueEncoding: 'binary' })

  await db.put('/a', '1', { keyEncoding: 'utf-8', valueEncoding: 'binary' })
  await db.put('/b', '2', { keyEncoding: 'utf-8', valueEncoding: 'json' })

  t.alike(await db.get('/a', { keyEncoding: 'utf-8', valueEncoding: 'binary' }), { seq: 1, key: '/a', value: b4a.from('1') })
  t.alike(await db.get('/b', { keyEncoding: 'utf-8', valueEncoding: 'json' }), { seq: 2, key: '/b', value: '2' })

  t.alike(await db.get('/a'), { seq: 1, key: b4a.from('/a'), value: b4a.from('1') })
  t.alike(await db.get('/b'), { seq: 2, key: b4a.from('/b'), value: b4a.from('"2"') })
})

test('sub encoding in the constructor', async function (t) {
  const { server, db } = await create(t, { keyEncoding: 'binary', valueEncoding: 'binary' })

  const keyEncodingA = new SubEncoder('a', 'utf-8')
  const keyEncodingB = new SubEncoder('b', 'utf-8')

  const db1 = new Protobee(server.key, server.clientSeed, { keyEncoding: keyEncodingA, valueEncoding: 'json', dht: db.dht })
  const db2 = new Protobee(server.key, server.clientSeed, { keyEncoding: keyEncodingB, valueEncoding: 'json', dht: db.dht })

  await db1.put('/a', '1')
  t.alike(await db1.get('/a'), { seq: 1, key: '/a', value: '1' })

  await db2.put('/b', '2')
  t.alike(await db2.get('/b'), { seq: 2, key: '/b', value: '2' })

  t.alike(await collect(db.createReadStream()), [
    { seq: 1, key: b4a.concat([b4a.from('a'), b4a.alloc(1), b4a.from('/a')]), value: b4a.from('"1"') },
    { seq: 2, key: b4a.concat([b4a.from('b'), b4a.alloc(1), b4a.from('/b')]), value: b4a.from('"2"') }
  ])

  await db1.close()
  await db2.close()
})
