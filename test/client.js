const test = require('brittle')
const Protobee = require('../index.js')
const { create } = require('./helpers/index.js')

test('multiple writer clients', async function (t) {
  const { server, db } = await create(t)

  const db1 = new Protobee(server.key, server.clientSeed, { dht: db.dht })
  t.is(db1.version, 1)
  await db1.ready()
  t.is(db1.version, 1)
  await db1.put('/a', '1')
  t.is(db1.version, 2)

  const db2 = new Protobee(server.key, server.clientSeed, { dht: db.dht })
  t.is(db2.version, 1)
  await db2.ready()
  t.is(db2.version, 2)
  await db2.put('/b', '2')
  t.is(db2.version, 3)

  const db3 = new Protobee(server.key, server.clientSeed, { dht: db.dht })
  t.is(db3.version, 1)
  await db3.ready()
  t.is(db3.version, 3)
  await db3.put('/c', '3')
  t.is(db3.version, 4)

  await db1.close()
  await db2.close()
  await db3.close()
})

test('basic auto reconnect', async function (t) {
  t.plan(1)

  const { db } = await create(t)

  db.rpc.destroy()
  await db.put('/a')

  db.rpc.destroy()
  t.alike(await db.get('/a'), { seq: 1, key: '/a', value: null })
})

test('auto reconnect on failures', async function (t) {
  t.plan(2)

  const { db } = await create(t)

  db.rpc.destroy()

  await db.put('/a')
  await db.put('/b')
  await db.put('/c')

  const actual = []

  try {
    db.rpc.destroy()

    for await (const entry of db.createReadStream()) {
      actual.push(entry.key)

      if (entry.key === '/b') {
        db.rpc.destroy()
      }
    }

    t.fail('Should have failed')
  } catch (err) {
    t.is(err.code, 'CHANNEL_DESTROYED')
  }

  t.alike(actual, ['/a', '/b'])

  await db.close()
})

test('reconnect should not throw if server is down', async function (t) {
  const { db } = await create(t)

  db.rpc.destroy()

  await db.put('/a')
})
