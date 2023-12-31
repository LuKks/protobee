// Probably there is a more straightforward way for this but don't know much about streams
module.exports = class StreamReader {
  constructor (protobee, rs, opts) {
    this._id = opts._id

    this.rs = rs
    this.readable = false
    this.ended = false
    this.destroyed = this.rs.destroyed

    this.rs.on('readable', this._onreadable.bind(this))
    this.rs.on('end', this._onend.bind(this))
    this.rs.on('close', this._onclose.bind(this))

    this.rs.setMaxListeners(0) // TODO: The static "wait" method is leaking atm due many concurrent reads from ProxyStream
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

  // TODO: This method is bad. Re-do it taking into account concurrent reads (e.g. prefetch in ProxyStream read)
  async read () {
    while (!this.readable) {
      if (this.ended || this.destroyed) return null

      const closed = await StreamReader.wait(this.rs) === false
      if (closed) return null
    }

    let data = this.rs.read()

    if (data === null) {
      this.readable = false
    }

    if (!this.ended && data === null) {
      data = await this.read()
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
