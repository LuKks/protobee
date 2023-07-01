const { Readable } = require('streamx')
const safetyCatch = require('safety-catch')

module.exports = class ProxyStream extends Readable {
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
