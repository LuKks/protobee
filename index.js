const ReadyResource = require('ready-resource')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
const DHT = require('hyperdht')
const ProtomuxRPC = require('protomux-rpc')
const debounceify = require('debounceify')
const retry = require('like-retry')
const sameObject = require('same-object')
const { Readable } = require('streamx')
const waitForRPC = require('./lib/wait-for-rpc.js')
const randomId = require('./lib/random-id.js')

module.exports = function (input, opts) {
  if (input.core) return new ProtobeeServer(input, opts)
  return new Protobee(input, opts)
}

class Protobee extends ReadyResource {
  constructor (keyPair, opts = {}) {
    super()

    this._keyPair = keyPair

    this.core = {
      key: this._keyPair.publicKey,
      keyPair: this._keyPair,
      length: 0
    }
    this.version = 1
    if (opts._sync) this._applySync(opts._sync)

    this._id = 0
    this._root = opts._root || null
    this._batch = !!opts._batch
    this._checkout = opts._checkout || null
    this._snapshot = opts._snapshot || null
    this._flushed = false

    this.dht = opts.dht || new DHT({ bootstrap: opts.bootstrap })
    this._autoDestroy = !opts.dht
    this._bootstrap = !opts.dht ? opts.bootstrap : undefined

    this.stream = null
    this.rpc = opts.rpc

    this.ready().catch(safetyCatch)
  }

  async _open () {
    if (this._batch) {
      const response = await this.rpc.request('batch')
      this._id = response.out
      this._applySync(response.sync)
      return
    }

    for await (const backoff of retry({ max: 1 })) {
      const socket = this.dht.connect(this._keyPair.publicKey, { keyPair: this._keyPair })

      const rpc = new ProtomuxRPC(socket, {
        id: this._keyPair.publicKey,
        valueEncoding: c.any
      })

      socket.setKeepAlive(5000)
      socket.userData = rpc.mux
      rpc.once('close', () => socket.destroy())

      rpc.respond('sync', debounceify(this._onsync.bind(this, rpc))) // It doesn't reply back (event) so it's safe to debounce

      try {
        await waitForRPC(rpc)
        this.rpc = rpc
        break
      } catch (err) {
        if (backoff.left === 0) this.close().catch(safetyCatch)
        await backoff(err)
      }
    }

    // TODO: this and the close above are needed due not sharing the DHT instance but needs to share it
    this.rpc.once('close', () => {
      this.close().catch(safetyCatch)
    })

    if (this._checkout) {
      const response = await this.rpc.request('checkout', { version: this._checkout.version, options: this._checkout.options })
      this._id = response.out
      this._applySync(response.sync)
      return
    }

    if (this._snapshot) {
      const response = await this.rpc.request('checkout', { version: this.version, options: this._snapshot.options })
      this._id = response.out
      this._applySync(response.sync)
      return
    }

    await this._update()
  }

  async _close () {
    // It's good to close explicitly, although it's not needed if server auto destroys client linked resources
    if (this._id && !this._flushed) {
      if (!this.rpc.closed) await this.rpc.request('close', { _id: this._id })
    }

    if (this._batch) return

    this.rpc.destroy()

    if (this._autoDestroy) await this.dht.destroy()
  }

  _unwrap (response) {
    this._applySync(response.sync)
    return response.out
  }

  _applySync (sync) {
    // TODO: what if there was a truncate or similar that reduces the version/length

    if (sync.bee.version > this.version) {
      this.version = sync.bee.version
    }

    if (sync.core.length > this.core.length) {
      this.core.length = sync.core.length
    }
  }

  _createSync (version) {
    return {
      core: {
        length: version ? version - 1 : this.core.length
      },
      bee: {
        version: version || this.version
      }
    }
  }

  // Debounced to avoid race conditions in case second update finishes first
  async _onsync (rpc, request) {
    await this.update()
  }

  // TODO: api method that does request + _id + error handling from the server response

  async _update () {
    if (this._batch) throw new Error('Update is only allowed from the main instance')

    const sync = await this.rpc.request('sync', { _id: this._id })
    this._applySync(sync)
    return false
  }

  async update () {
    if (this.opened === false) await this.opening

    return this._update()
  }

  async put (key, value, opts) {
    if (this.opened === false) await this.opening
    if (this._id && !this._batch) throw new Error('Can not put from a snapshot')
    if (opts && typeof opts.cas === 'function') throw new Error('Option cas as function is not supported')
    const cas = !!(opts && opts.cas)

    return this._unwrap(await this.rpc.request('put', { _id: this._id, key, value, cas }))
  }

  async get (key) {
    if (this.opened === false) await this.opening

    return this._unwrap(await this.rpc.request('get', { _id: this._id, key }))
  }

  async del (key, opts) {
    if (this.opened === false) await this.opening
    if (this._id && !this._batch) throw new Error('Can not del from a snapshot')
    if (opts && opts.cas) throw new Error('CAS option for del is not supported') // There is no good default, and dangerous to run a custom func remotely

    return this._unwrap(await this.rpc.request('del', { _id: this._id, key }))
  }

  async peek (range, options) {
    if (this.opened === false) await this.opening

    return this._unwrap(await this.rpc.request('peek', { _id: this._id, range, options }))
  }

  batch () {
    if (this._id) throw new Error('Batch is only allowed from the main instance')

    return new Protobee(this._keyPair, {
      _root: this,
      dht: this.dht,
      rpc: this.rpc,
      _batch: true,
      _sync: this._createSync()
    })
  }

  async lock () {
    if (this.opened === false) await this.opening
    if (!this._batch) throw new Error('Lock is only allowed from a batch instance')

    return this._unwrap(await this.rpc.request('lock', { _id: this._id }))
  }

  async flush () {
    if (this.opened === false) await this.opening
    if (!this._batch) throw new Error('Flush is only allowed from a batch instance')

    const r = await this.rpc.request('flush', { _id: this._id })
    this._flushed = true

    // Note: apply sync into root bee
    this._root._unwrap(r)

    return this.close()
  }

  createReadStream (range, options) {
    return new ProxyStream(this, 'read-stream', { range, options })
  }

  createHistoryStream (options) {
    return new ProxyStream(this, 'history-stream', { options })
  }

  createDiffStream (otherVersion, range, options) {
    if (typeof otherVersion === 'object') otherVersion = otherVersion.version

    return new ProxyStream(this, 'diff-stream', { otherVersion, range, options })
  }

  checkout (version, options) {
    if (this._id) throw new Error('Checkout is only allowed from the main instance')

    return new Protobee(this._keyPair, {
      bootstrap: this._bootstrap, // TODO: it should share the same DHT instance but without auto destroying it if the main protobee instance closes
      _checkout: { version, options },
      _sync: this._createSync(version)
    })
  }

  snapshot (options) {
    if (this._id) throw new Error('Snapshot is only allowed from the main instance')

    return new Protobee(this._keyPair, {
      bootstrap: this._bootstrap,
      _snapshot: { options },
      _sync: this._createSync()
    })
  }

  async getHeader (options) {
    if (this.opened === false) await this.opening

    return this._unwrap(await this.rpc.request('getHeader', { _id: this._id, options }))
  }
}

class ProtobeeServer extends ReadyResource {
  constructor (bee, opts = {}) {
    super()

    this.core = bee.core
    this.bee = bee

    this.dht = opts.dht || new DHT({ bootstrap: opts.bootstrap })
    this._autoDestroy = !opts.dht

    this.server = null
    this.connections = new Set()
    this.instances = new Resources()
    this.streams = new Resources()

    this.core.on('append', this._onappend.bind(this))

    this.ready().catch(safetyCatch)
  }

  async _open () {
    await this.bee.ready()

    if (!this.core.writable) throw new Error('Hyperbee must be writable')

    this.server = this.dht.createServer({ firewall: this._onfirewall.bind(this) })
    this.server.on('connection', this._onconnection.bind(this))
    await this.server.listen(this.core.keyPair)
  }

  async _close () {
    await this.server.close()
    if (this._autoDestroy) await this.dht.destroy()

    await this.bee.close()
  }

  _onfirewall (publicKey, remotePayload, from) {
    return !this.core.keyPair.publicKey.equals(publicKey)
  }

  _onconnection (socket) {
    const rpc = new ProtomuxRPC(socket, {
      id: this.server.publicKey,
      valueEncoding: c.any
    })

    socket.userData = rpc.mux
    socket.setKeepAlive(5000)
    rpc.once('close', () => socket.destroy())

    rpc.respond('sync', this.onsync.bind(this, rpc))

    rpc.respond('put', this.onput.bind(this, rpc))
    rpc.respond('get', this.onget.bind(this, rpc))
    rpc.respond('del', this.ondel.bind(this, rpc))
    rpc.respond('peek', this.onpeek.bind(this, rpc))

    rpc.respond('checkout', this.oncheckout.bind(this, rpc))
    rpc.respond('snapshot', this.onsnapshot.bind(this, rpc))

    rpc.respond('batch', this.onbatch.bind(this, rpc))
    rpc.respond('lock', this.onlock.bind(this, rpc))
    rpc.respond('flush', this.onflush.bind(this, rpc))

    rpc.respond('read-stream', this.onreadstream.bind(this, rpc))
    rpc.respond('history-stream', this.onhistorystream.bind(this, rpc))
    rpc.respond('diff-stream', this.ondiffstream.bind(this, rpc))
    rpc.respond('stream-read', this.onstreamread.bind(this, rpc))
    rpc.respond('stream-destroy', this.onstreamdestroy.bind(this, rpc))

    rpc.respond('getHeader', this.ongetheader.bind(this, rpc))
    rpc.respond('close', this.onclose.bind(this, rpc))

    this.connections.add(rpc)

    rpc.once('close', () => {
      this.connections.delete(rpc)
      this._ondisconnection(rpc) // Runs on background, it doesn't crash
    })
  }

  async _ondisconnection (rpc, resources) {
    while (true) {
      const instance = this.instances.shift(rpc)
      if (instance === null) break
      await instance.close().catch(safetyCatch)
    }

    while (true) {
      const stream = this.streams.shift(rpc)
      if (stream === null) break
      await stream.destroy().catch(safetyCatch)
    }
  }

  _onappend () {
    // TODO: should detect 'append' events individually per checkout per client, etc?

    for (const rpc of this.connections) {
      rpc.event('sync') // TODO: I think it could send sync data here, but be aware of truncates first
    }
  }

  _bee (request, rpc) {
    if (request && request._id) return this.instances.get(rpc, request._id)
    return this.bee
  }

  _wrap (out, request, rpc) {
    return {
      out,
      sync: this.onsync(rpc, request)
    }
  }

  onsync (rpc, request) {
    return {
      core: {
        length: this._bee(request, rpc).core.length
      },
      bee: {
        version: this._bee(request, rpc).version
      }
    }
  }

  // TODO: forward error as response

  async onput (rpc, request) {
    const cas = request.cas ? defaultCasPut : null
    return this._wrap(await this._bee(request, rpc).put(request.key, request.value, { cas }), request, rpc)
  }

  async onget (rpc, request) {
    return this._wrap(await this._bee(request, rpc).get(request.key), request, rpc)
  }

  async ondel (rpc, request) {
    return this._wrap(await this._bee(request, rpc).del(request.key), request, rpc)
  }

  async onpeek (rpc, request) {
    return this._wrap(await this._bee(request, rpc).peek(request.range, request.options), request, rpc)
  }

  async onbatch (rpc, request) {
    const batch = this.bee.batch()
    const id = this.instances.add(rpc, batch)

    return this._wrap(id, { _id: id }, rpc)
  }

  async onlock (rpc, request) {
    // TODO: should check and add protections so server doesn't crash if there is a bad client like forcing .lock() on non-batch, same for others
    return this._wrap(await this._bee(request, rpc).lock(), request, rpc)
  }

  async onflush (rpc, request) {
    const batch = this.instances.get(rpc, request._id)
    if (!batch) return this._wrap()

    this.instances.delete(rpc, request._id)
    await batch.flush()

    return this._wrap()
  }

  async onreadstream (rpc, request) {
    const stream = this._bee(request, rpc).createReadStream(request.range || undefined, request.options || undefined)
    const reader = new StreamReader(this, stream, { _id: request._id })
    const id = this.streams.add(rpc, reader)

    return this._wrap(id, request, rpc)
  }

  async onhistorystream (rpc, request) {
    const stream = this._bee(request, rpc).createHistoryStream(request.options || undefined)
    const reader = new StreamReader(this, stream, { _id: request._id })
    const id = this.streams.add(rpc, reader)

    return this._wrap(id, request, rpc)
  }

  async ondiffstream (rpc, request) {
    const stream = this._bee(request, rpc).createDiffStream(request.otherVersion, request.range || undefined, request.options || undefined)
    const reader = new StreamReader(this, stream, { _id: request._id })
    const id = this.streams.add(rpc, reader)

    return this._wrap(id, request, rpc)
  }

  async onstreamread (rpc, request) {
    const reader = this.streams.get(rpc, request._streamId)
    if (!reader) return this._wrap(undefined, request, rpc)

    const value = await reader.read()

    return this._wrap({ value, ended: reader.ended }, request, rpc)
  }

  async onstreamdestroy (rpc, request) {
    const reader = this.streams.get(rpc, request._streamId)
    if (!reader) return this._wrap(undefined, request, rpc)

    this.streams.delete(rpc, request._streamId)
    await reader.destroy()

    return this._wrap(undefined, request, rpc)
  }

  // TODO: getAndWatch
  // TODO: watch

  async oncheckout (rpc, request) {
    const checkout = this.bee.checkout(request.version, request.options || {})
    const id = this.instances.add(rpc, checkout)

    return this._wrap(id, { _id: id }, rpc)
  }

  async onsnapshot (rpc, request) {
    const snapshot = this.bee.snapshot(request.options || {})
    const id = this.instances.add(rpc, snapshot)

    return this._wrap(id, { _id: id }, rpc)
  }

  async ongetheader (rpc, request) {
    return this._wrap(await this._bee(request, rpc).getHeader(request.options || {}), request, rpc)
  }

  async onclose (rpc, request) {
    const checkout = this.instances.get(rpc, request._id)
    if (!checkout) return this._wrap()

    this.instances.delete(rpc, request._id)
    await checkout.close()

    return this._wrap()
  }
}

class Resources {
  constructor () {
    this.all = new Map()
    this.clients = new Map()
  }

  get size () {
    return this.all.size
  }

  add (rpc, value) {
    const id = randomId((id) => this.all.has(id))

    this.all.set(id, {
      rpc,
      value
    })

    const client = this.clients.get(rpc)
    if (!client) this.clients.set(rpc, [id])
    else client.push(id)

    return id
  }

  get (rpc, id) {
    const resource = this.all.get(id)
    if (!resource) return null
    if (resource.rpc !== rpc) throw new Error('RPC does not match the one from resource')

    return resource.value
  }

  delete (rpc, id) {
    const client = this.clients.get(rpc)
    if (!client) throw new Error('Client not found')

    const resource = this.all.get(id)
    if (!resource) throw new Error('Resource not found')
    if (resource.rpc !== rpc) throw new Error('RPC does not match the one from resource')

    this.all.delete(id)

    const i = client.indexOf(id)
    if (i > -1) client.splice(i, 1)
    if (client.length === 0) this.clients.delete(rpc)
  }

  shift (rpc) {
    const client = this.clients.get(rpc)
    if (!client) return null

    const id = client.shift()
    if (client.length === 0) this.clients.delete(rpc)

    const value = this.get(rpc, id)
    this.all.delete(id)

    return value
  }
}

class ProxyStream extends Readable {
  constructor (db, type, args) {
    super()

    this.db = db

    this.type = type
    this.args = args

    this.streamId = null
  }

  _open (cb) { this._openp().then(cb, cb) }
  _read (cb) { this._readp().then(cb, cb) }
  _destroy (cb) { this._destroyp().then(cb, cb) }

  async _openp () {
    if (this.db.opened === false) await this.db.opening.catch(safetyCatch)

    this.streamId = this.db._unwrap(await this.db.rpc.request(this.type, { _id: this.db._id, ...this.args }))
  }

  async _readp () {
    const { value, ended } = this.db._unwrap(await this.db.rpc.request('stream-read', { _id: this.db._id, _streamId: this.streamId }))

    this.push(value)
    if (ended) this.push(null)
  }

  async _destroyp () {
    if (this.db.rpc.closed) return

    this.db._unwrap(await this.db.rpc.request('stream-destroy', { _id: this.db._id, _streamId: this.streamId }))
  }
}

// Probably there is a more straightforward way for this but don't know much about streams
class StreamReader {
  constructor (protobee, rs, opts) {
    this._id = opts._id

    this.rs = rs
    this.readable = false
    this.ended = false
    this.destroyed = this.rs.destroyed

    this.rs.on('readable', this._onreadable.bind(this))
    this.rs.on('end', this._onend.bind(this))
    this.rs.on('close', this._onclose.bind(this))
  }

  _onreadable () {
    this.readable = true
  }

  _onend () {
    this.ended = true
  }

  _onclose () {
    this.destroyed = true
  }

  async read () {
    while (!this.readable) {
      if (this.ended || this.destroyed) return null

      const closed = await StreamReader.wait(this.rs) === false
      if (closed) return null
    }

    const data = this.rs.read()

    if (data === null) {
      this.readable = false
    }

    return data
  }

  async destroy () {
    this.rs.destroy()

    if (!this.destroyed) {
      await new Promise(resolve => this.rs.once('close', resolve))
    }
  }

  static wait (rs) {
    return new Promise(resolve => {
      rs.on('readable', onreadable)
      rs.on('end', onclose)
      rs.on('close', onclose)

      function cleanup () {
        rs.off('readable', onreadable)
        rs.off('end', onclose)
        rs.off('close', onclose)
      }

      function onreadable () {
        cleanup()
        resolve(true)
      }

      function onclose () {
        cleanup()
        resolve(false)
      }
    })
  }
}

function defaultCasPut (prev, next) {
  return !sameObject(prev.value, next.value, { strict: true })
}
