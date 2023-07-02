const ReadyResource = require('ready-resource')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
const DHT = require('hyperdht')
const ProtomuxRPC = require('protomux-rpc')
const debounceify = require('debounceify')
const retry = require('like-retry')
const crypto = require('hypercore-crypto')
const waitForRPC = require('./lib/wait-for-rpc.js')
const ProxyStream = require('./lib/proxy-stream.js')

module.exports = class Protobee extends ReadyResource {
  constructor (serverPublicKey, primaryKey, opts = {}) {
    super()

    this.serverPublicKey = serverPublicKey
    this.primaryKey = primaryKey
    this._keyPair = crypto.keyPair(this.primaryKey)

    this.core = {
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
      const socket = this.dht.connect(this.serverPublicKey, { keyPair: this._keyPair })

      const rpc = new ProtomuxRPC(socket, {
        id: this.serverPublicKey,
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

    return new Protobee(this.serverPublicKey, this.primaryKey, {
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

    return new Protobee(this.serverPublicKey, this.primaryKey, {
      bootstrap: this._bootstrap, // TODO: it should share the same DHT instance but without auto destroying it if the main protobee instance closes
      _checkout: { version, options },
      _sync: this._createSync(version)
    })
  }

  snapshot (options) {
    if (this._id) throw new Error('Snapshot is only allowed from the main instance')

    return new Protobee(this.serverPublicKey, this.primaryKey, {
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
