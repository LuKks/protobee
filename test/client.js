const test = require('brittle')
const Protobee = require('../index.js')
const { create } = require('./helpers/index.js')

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
