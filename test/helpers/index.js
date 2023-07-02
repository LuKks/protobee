const Protobee = require('../../index.js')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const createTestnet = require('hyperdht/testnet')
const RAM = require('random-access-memory')
const c = require('compact-encoding')

module.exports = {
  create
}

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
