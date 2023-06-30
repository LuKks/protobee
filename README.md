# protobee

Hyperbee over Protomux RPC. Primary server that allows many writable clients

```
npm i protobee
```

## Usage

```js
const Protobee = require('protobee')

const core = new Hypercore(RAM)
const bee = new Hyperbee(core, { keyEncoding: c.any, valueEncoding: c.any })
await bee.ready()

const server = new Protobee.Server(bee)
await server.ready()

const db = new Protobee(core.keyPair)
await db.ready()

console.log(db.version) // => 1
await db.put('/a', '1')
console.log(db.version) // => 2

const db2 = new Protobee(core.keyPair)
await db2.ready()

console.log(db2.version) // => 2
await db2.get('/a') // => { seq, key, value }
```

## License

MIT
