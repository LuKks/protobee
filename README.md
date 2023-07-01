# protobee

Hyperbee over Protomux. Primary server that allows many writable clients

```
npm i protobee
```

Warning: this is experimental, and it might contain edge case bugs.

## Usage

```js
const RAM = require('random-access-memory')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const Protobee = require('protobee')

// Server side
const core = new Hypercore(RAM)
const bee = new Hyperbee(core, { keyEncoding: c.any, valueEncoding: c.any })

const server = new Protobee(bee)
await server.ready()

// Client side
const db = new Protobee(core.keyPair)
await db.ready()

console.log(db.version) // => 1
await db.put('/a', '1')
console.log(db.version) // => 2

// Another client
const db2 = new Protobee(core.keyPair)
await db2.ready()

console.log(db2.version) // => 2
await db2.get('/a') // => { seq, key, value }
```

## API

#### `const server = new Protobee(bee, [options])`

Creates a DHT server that handles RPC requests supporting the Hyperbee API.

Available `options`:
```js
{
  dht, // DHT instance
  bootstrap // Array of bootstrap nodes (only if you didn't pass a DHT instance)
}
```

#### `await server.ready()`

Wait for the server to be listening on the Hypercore key pair of the Hyperbee.

#### `const db = new Protobee(keyPair, [options])`

Creates a RPC client to connects to the server.

`keyPair` must be the Hypercore key pair of the Hyperbee:
```js
{
  publicKey,
  secretKey
}
```

Available `options`:
```js
{
  dht, // DHT instance
  bootstrap // Array of bootstrap nodes (only if you didn't pass a DHT instance)
}
```

#### The rest of the API

Practically the same API as Hyperbee:

https://github.com/holepunchto/hyperbee

## Differences with Hyperbee

- The way you create the Protobee instance is different but this is expected.
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
