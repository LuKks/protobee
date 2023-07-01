const randomId = require('./random-id.js')

module.exports = class Resources {
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
