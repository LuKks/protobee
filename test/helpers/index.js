const Protobee = require('../../index.js')
const fs = require('fs')
const path = require('path')
const os = require('os')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const createTestnet = require('hyperdht/testnet')
const RAM = require('random-access-memory')
const c = require('compact-encoding')

module.exports = {
  create,
  createTmpDir,
  collect
}

async function create (t, opts = {}) {
  const testnet = await createTestnet()
  const bootstrap = testnet.bootstrap
  t.teardown(() => testnet.destroy(), { order: Infinity })

  const core = opts.core || new Hypercore(RAM)
  const bee = new Hyperbee(core)

  const server = new Protobee.Server(bee, { bootstrap, primaryKey: opts.primaryKey })
  await server.ready()
  t.teardown(() => server.close())

  await core.ready()
  if (opts.data) await bee.put('/test', 'abc')

  const db = new Protobee(server.key, server.clientSeed, { bootstrap, keyEncoding: opts.keyEncoding || c.any, valueEncoding: opts.valueEncoding || c.any })
  if (opts.ready !== false) await db.ready()
  t.teardown(() => db.close())

  return { server, db }
}

function createTmpDir (t) {
  const tmpdir = path.join(os.tmpdir(), 'protobee-test-')
  const dir = fs.mkdtempSync(tmpdir)
  t.teardown(() => fs.promises.rm(dir, { recursive: true }))
  return dir
}

async function collect (stream) {
  const actual = []
  for await (const entry of stream) {
    actual.push(entry)
  }
  return actual
}
