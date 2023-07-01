# protobee

Hyperbee over Protomux. Primary server that allows many writable clients

```
npm i protobee
```

Warning: this is experimental, and it might contain edge case bugs.

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

## Differences with Hyperbee

- Errors are very different at the moment.
- Possible bugs around the `version` property due core truncates (needs more debugging and testing).
- Key and value encodings are not the same because we can't allow passing custom encoding functions.
- In `bee.put` method the `cas` option is a bool, and if you pass a function it will throw.
- In `bee.del` method the `cas` option is not supported, and it will throw if you use it.

Pretty sure that we can find a really good middle ground for the encodings.

Needs more debugging to confirm that there are no leaks around all the instances and streams that gets created.

A bad client could mess up the server by creating unlimited snapshots or sending bad requests (needs protection settings).

Current unsupported methods:

- `sub` (in favor of using sub-encoder lib, but it might end up being supported)
- `watch`
- `getAndWatch`

## License

MIT
