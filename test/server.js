const test = require('brittle')
const { create } = require('./helpers/index.js')

test('server resources', async function (t) {
  const { server, db } = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')

  t.is(server.connections.size, 1)
  t.is(server.instances.size, 0)
  t.is(server.streams.size, 0)

  const batch = db.batch()
  await batch.ready()
  t.is(server.connections.size, 1)
  t.is(server.instances.size, 1)
  t.is(server.streams.size, 0)

  const snap = db.snapshot()
  await snap.ready()
  t.is(server.connections.size, 2)
  t.is(server.instances.size, 2)
  t.is(server.streams.size, 0)

  const checkout = db.checkout(2)
  await checkout.ready()
  t.is(server.connections.size, 3)
  t.is(server.instances.size, 3)
  t.is(server.streams.size, 0)

  const stream = db.createHistoryStream()
  await new Promise(resolve => stream.once('readable', resolve))
  t.is(server.connections.size, 3)
  t.is(server.instances.size, 3)
  t.is(server.streams.size, 1)

  await batch.close()
  t.is(server.connections.size, 3)
  t.is(server.instances.size, 2)
  t.is(server.streams.size, 1)

  await snap.close()
  await new Promise(resolve => setTimeout(resolve, 50))
  t.is(server.connections.size, 2)
  t.is(server.instances.size, 1)
  t.is(server.streams.size, 1)

  await checkout.close()
  await new Promise(resolve => setTimeout(resolve, 50))
  t.is(server.connections.size, 1)
  t.is(server.instances.size, 0)
  t.is(server.streams.size, 1)

  stream.destroy()
  await new Promise(resolve => stream.once('close', resolve))
  await new Promise(resolve => setTimeout(resolve, 50))
  t.is(server.connections.size, 1)
  t.is(server.instances.size, 0)
  t.is(server.streams.size, 0)

  await db.close()
  await new Promise(resolve => setTimeout(resolve, 50))
  t.is(server.connections.size, 0)
})

test('destroying client should destroy linked resources in server', async function (t) {
  const { server, db } = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')

  t.is(server.instances.size, 0)
  t.is(server.streams.size, 0)

  const batch = db.batch()
  const snap = db.snapshot()
  const checkout = db.checkout(2)
  const stream = db.createHistoryStream()

  await new Promise(resolve => stream.once('readable', resolve))
  await batch.ready()
  await snap.ready()
  await checkout.ready()

  t.is(server.instances.size, 3)
  t.is(server.streams.size, 1)

  batch.rpc.destroy()
  snap.rpc.destroy()
  checkout.rpc.destroy()
  stream.db.rpc.destroy()
  await new Promise(resolve => setTimeout(resolve, 50))

  t.is(server.instances.size, 0)
  t.is(server.streams.size, 0)

  await batch.close()
  await snap.close()
  await checkout.close()
  stream.destroy()
  await new Promise(resolve => stream.once('close', resolve))
})
