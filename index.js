const ReadyResource = require('ready-resource')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
const DHT = require('hyperdht')
const ProtomuxRPC = require('protomux-rpc')
const debounceify = require('debounceify')
const retry = require('like-retry')
const sameObject = require('same-object')
const waitForRPC = require('./lib/wait-for-rpc.js')
const randomId = require('./lib/random-id.js')

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

    if (this._checkout) {
      const response = await this.rpc.request('checkout', { version: this._checkout.version, options: this._checkout.options })
      this._id = response.out
      this._applySync(response.sync)
      return
    }

    if (this._snapshot) {
      const response = await this.rpc.request('snapshot', { options: this._snapshot.options })
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

      rpc.respond('sync', debounceify(this._onsync.bind(this))) // It doesn't reply back (event) so it's safe to debounce

      try {
        await waitForRPC(rpc)
        this.rpc = rpc
        break
      } catch (err) {
        await backoff(err)
      }
    }

    await this._update()
  }

  async _close () {
    if (this._id) {
      if (!this._flushed) {
        await this.rpc.request('close', { _id: this._id })
      }
      return
    }

    await this.rpc.destroy()

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
  async _onsync (request, rpc) {
    await this.update()
  }

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

  checkout (version, options) {
    if (this._id) throw new Error('Checkout is only allowed from the main instance')

    return new Protobee(this._keyPair, {
      dht: this.dht,
      rpc: this.rpc,
      _checkout: { version, options },
      _sync: this._createSync(version)
    })
  }

  snapshot (options) {
    if (this._id) throw new Error('Snapshot is only allowed from the main instance')

    // TODO: it should be new independent client RPCs I think, same for others. Because closing the main instance will invalidate all snapshots, etc while that is not true in Hyperbee
    return new Protobee(this._keyPair, {
      dht: this.dht,
      rpc: this.rpc,
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

    this.checkouts = new Map()

    this.core.on('append', this._onappend.bind(this))
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

    for (const [id, checkout] of this.checkouts) {
      this.checkouts.delete(id)
      await checkout.close()
    }
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

    rpc.respond('sync', this.onsync.bind(this))

    rpc.respond('put', this.onput.bind(this))
    rpc.respond('get', this.onget.bind(this))
    rpc.respond('del', this.ondel.bind(this))
    rpc.respond('peek', this.onpeek.bind(this))

    rpc.respond('checkout', this.oncheckout.bind(this))
    rpc.respond('snapshot', this.onsnapshot.bind(this))

    rpc.respond('batch', this.onbatch.bind(this))
    rpc.respond('lock', this.onlock.bind(this))
    rpc.respond('flush', this.onflush.bind(this))

    rpc.respond('getHeader', this.ongetheader.bind(this))
    rpc.respond('close', this.onclose.bind(this))

    this.connections.add(rpc)
    rpc.once('close', () => this.connections.delete(rpc))
  }

  _onappend () {
    for (const rpc of this.connections) {
      rpc.event('sync')
    }
  }

  _bee (request) {
    if (request && request._id) return this.checkouts.get(request._id)
    return this.bee
  }

  _wrap (out, request) {
    return {
      out,
      sync: this.onsync(request)
    }
  }

  onsync (request, rpc) {
    return {
      core: {
        length: this._bee(request).core.length
      },
      bee: {
        version: this._bee(request).version
      }
    }
  }

  async onput (request, rpc) {
    const cas = request.cas ? defaultCasPut : null
    return this._wrap(await this._bee(request).put(request.key, request.value, { cas }), request)
  }

  async onget (request, rpc) {
    return this._wrap(await this._bee(request).get(request.key), request)
  }

  async ondel (request, rpc) {
    return this._wrap(await this._bee(request).del(request.key), request)
  }

  async onpeek (request, rpc) {
    return this._wrap(await this._bee(request).peek(request.range, request.options), request)
  }

  async onbatch (request, rpc) {
    const id = randomId((id) => this.checkouts.has(id))
    const batch = this.bee.batch()

    this.checkouts.set(id, batch)
    // batch.once('close', () => this.checkouts.delete(id)) // Batch does not have 'close' event to clear itself

    return this._wrap(id, { _id: id })
  }

  async onlock (request, rpc) {
    // TODO: should check and add protections so server doesn't crash if there is a bad client like forcing .lock() on non-batch, same for others
    return this._wrap(await this._bee(request).lock(), request)
  }

  async onflush (request, rpc) {
    const batch = this.checkouts.get(request._id)
    if (!batch) return

    this.checkouts.delete(request._id) // Until batch have a 'close' event
    await batch.flush()

    return this._wrap()
  }

  // TODO: createReadStream
  // TODO: createHistoryStream
  // TODO: createDiffStream
  // TODO: getAndWatch
  // TODO: watch

  async oncheckout (request, rpc) {
    const id = randomId((id) => this.checkouts.has(id))
    const checkout = this.bee.checkout(request.version, request.options || {})

    this.checkouts.set(id, checkout)
    checkout.once('close', () => this.checkouts.delete(id))

    return this._wrap(id, { _id: id })
  }

  async onsnapshot (request, rpc) {
    const id = randomId((id) => this.checkouts.has(id))
    const snapshot = this.bee.snapshot(request.options || {})

    this.checkouts.set(id, snapshot)
    snapshot.once('close', () => this.checkouts.delete(id))

    return this._wrap(id, { _id: id })
  }

  async ongetheader (request, rpc) {
    return this._wrap(await this._bee(request).getHeader(request.options || {}), request)
  }

  async onclose (request, rpc) {
    const checkout = this.checkouts.get(request._id)
    if (!checkout) return

    this.checkouts.delete(request._id) // Batch does not have 'close' event to clear itself
    await checkout.close()

    // Note: it syncs root bee with client
    return this._wrap()
  }
}

Protobee.Server = ProtobeeServer

module.exports = Protobee

function defaultCasPut (prev, next) {
  return !sameObject(prev.value, next.value, { strict: true })
}
