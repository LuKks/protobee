const ReadyResource = require('ready-resource')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
const DHT = require('hyperdht')
const ProtomuxRPC = require('protomux-rpc')
const sameObject = require('same-object')
const StreamReader = require('./lib/stream-reader.js')
const Resources = require('./lib/resources.js')

module.exports = class ProtobeeServer extends ReadyResource {
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

function defaultCasPut (prev, next) {
  return !sameObject(prev.value, next.value, { strict: true })
}
