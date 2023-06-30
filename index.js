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
      length: 0
    }
    this.version = 1
    if (opts._sync) this._applySync(opts._sync)

    this._root = opts._root || null
    this._batch = !!opts._batch
    this._checkout = opts._checkout || 0
    this._flushed = false

    this.dht = opts.dht || new DHT({ bootstrap: opts.bootstrap })
    this._autoDestroy = !opts.dht

    this.stream = null
    this.rpc = opts.rpc

    this.ready().catch(safetyCatch)
  }

  async _open () {
    if (this._checkout) return

    for await (const backoff of retry({ max: 1 })) {
      const socket = this.dht.connect(this._keyPair.publicKey, { keyPair: this._keyPair })

      const rpc = new ProtomuxRPC(socket, {
        id: this._keyPair.publicKey,
        valueEncoding: c.any
        /* handshakeEncoding: c.any,
        handshake: {
          keyPair: {
            publicKey: this._keyPair.publicKey,
            secretKey: this._keyPair.secretKey
          }
        } */
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

    await this.update()
  }

  async _close () {
    if (this._checkout) {
      if (!this._flushed) {
        await this.rpc.request('close', { _checkout: this._checkout })
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

  // Debounced to avoid race conditions in case second update finishes first
  async _onsync (request, rpc) {
    await this.update()
  }

  async update () {
    if (this._batch) throw new Error('Update is only allowed from the main instance')

    const sync = await this.rpc.request('sync', { _checkout: this._checkout })
    this._applySync(sync)
    return false
  }

  async put (key, value, opts) {
    if (this._checkout && !this._batch) throw new Error('Can not put from a snapshot')
    const cas = !!(opts && opts.cas)

    return this._unwrap(await this.rpc.request('put', { _checkout: this._checkout, key, value, cas }))
  }

  async get (key) {
    return this.rpc.request('get', { _checkout: this._checkout, key })
  }

  async del (key, opts) {
    if (this._checkout && !this._batch) throw new Error('Can not del from a snapshot')
    if (opts && opts.cas) throw new Error('CAS option for del is not supported') // There is no good default, and dangerous to run a custom func remotely

    return this._unwrap(await this.rpc.request('del', { _checkout: this._checkout, key }))
  }

  async peek (range, options) {
    return this.rpc.request('peek', { _checkout: this._checkout, range, options })
  }

  async batch () {
    if (this._checkout) throw new Error('Batch is only allowed from the main instance')

    const response = await this.rpc.request('batch')

    return new Protobee(this._keyPair, {
      _root: this,
      dht: this.dht,
      rpc: this.rpc,
      _batch: true,
      _checkout: response.out,
      _sync: response.sync
    })
  }

  async lock () {
    if (!this._batch) throw new Error('Lock is only allowed from a batch instance')

    return this.rpc.request('lock', { _checkout: this._checkout })
  }

  async flush () {
    if (!this._batch) throw new Error('Flush is only allowed from a batch instance')

    const r = await this.rpc.request('flush', { _checkout: this._checkout })
    this._flushed = true

    // Note: apply sync into root bee
    this._root._unwrap(r)

    return this.close()
  }

  async checkout (version, options) {
    if (this._checkout) throw new Error('Checkout is only allowed from the main instance')

    const response = await this.rpc.request('checkout', { version, options })

    return new Protobee(this._keyPair, {
      dht: this.dht,
      rpc: this.rpc,
      _checkout: response.out,
      _sync: response.sync
    })
  }

  async snapshot (options) {
    if (this._checkout) throw new Error('Snapshot is only allowed from the main instance')

    const response = await this.rpc.request('snapshot', { options })

    // TODO: it should be new independent client RPCs I think, same for others. Because closing the main instance will invalidate all snapshots, etc while that is not true in Hyperbee
    return new Protobee(this._keyPair, {
      dht: this.dht,
      rpc: this.rpc,
      _checkout: response.out,
      _sync: response.sync
    })
  }

  async getHeader (options) {
    return this.rpc.request('getHeader', { _checkout: this._checkout, options })
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

    if (!this.core.writable) throw new Error('Must be writable')

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
      /* handshakeEncoding: c.any,
      handshake: {
        keyPair: {
          publicKey: Buffer.alloc(0),
          secretKey: Buffer.alloc(0)
        }
      } */
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
      rpc.event('sync') // TODO: send data here
    }
  }

  _bee (request) {
    if (request && request._checkout) return this.checkouts.get(request._checkout)
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
    return this._bee(request).get(request.key)
  }

  async ondel (request, rpc) {
    return this._wrap(await this._bee(request).del(request.key), request)
  }

  async onpeek (request, rpc) {
    return this._bee(request).peek(request.range, request.options)
  }

  async onbatch (request, rpc) {
    const id = randomId((id) => this.checkouts.has(id))
    const batch = this.bee.batch()

    this.checkouts.set(id, batch)
    // batch.once('close', () => this.checkouts.delete(id)) // Batch does not have 'close' event to clear itself

    return this._wrap(id, { _checkout: id })
  }

  async onlock (request, rpc) {
    // TODO: should check and add protections so server doesn't crash if there is a bad client like forcing .lock() on non-batch, same for others
    return this._bee(request).lock()
  }

  async onflush (request, rpc) {
    const batch = this.checkouts.get(request._checkout)
    if (!batch) return

    this.checkouts.delete(request._checkout) // Until batch have a 'close' event
    await batch.flush()

    // Note: it syncs root bee with client
    return this._wrap()
  }

  // TODO: getAndWatch
  // TODO: watch

  async oncheckout (request, rpc) {
    const id = randomId((id) => this.checkouts.has(id))
    const checkout = this.bee.checkout(request.version, request.options || {})

    this.checkouts.set(id, checkout)
    checkout.once('close', () => this.checkouts.delete(id))

    return this._wrap(id, { _checkout: id })
  }

  async onsnapshot (request, rpc) {
    const id = randomId((id) => this.checkouts.has(id))
    const snapshot = this.bee.snapshot(request.options || {})

    this.checkouts.set(id, snapshot)
    snapshot.once('close', () => this.checkouts.delete(id))

    return this._wrap(id, { _checkout: id })
  }

  async ongetheader (request, rpc) {
    return this._bee(request).getHeader(request.options || {})
  }

  async onclose (request, rpc) {
    const checkout = this.checkouts.get(request._checkout)
    if (!checkout) return

    this.checkouts.delete(request._checkout) // Batch does not have 'close' event to clear itself
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