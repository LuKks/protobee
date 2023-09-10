# protobee

Hyperbee over Protomux. Primary server that allows many writable clients

```
npm i protobee
```

Warning: this is experimental, and it might contain edge case bugs.

## Usage

```js
const RAM = require('random-access-memory')
const c = require('compact-encoding')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const Protobee = require('protobee')

// Server side
const core = new Hypercore(RAM)
const bee = new Hyperbee(core)

const server = new Protobee.Server(bee)
await server.ready()

// Client side
const db = new Protobee(server.key, server.clientSeed, { keyEncoding: c.any, valueEncoding: c.any })
await db.ready()

console.log(db.version) // => 1
await db.put('/a', '1')
console.log(db.version) // => 2

// Another client
const db2 = new Protobee(server.key, server.clientSeed)
await db2.ready()

console.log(db2.version) // => 2
await db2.get('/a') // => { seq, key, value }
```

## API

#### `const server = new Protobee.Server(bee, [options])`

Creates a DHT server that handles RPC requests supporting the Hyperbee API.

`bee` must be a Hyperbee instance.

Available `options`:
```js
{
  primaryKey, // Secret primary key used to derive server and client key pairs
  dht, // DHT instance
  bootstrap // Array of bootstrap nodes (only if you didn't pass a DHT instance)
}
```

#### `await server.ready()`

Wait for the server to be listening on the Hypercore key pair of the Hyperbee.

#### `server.key`

Public key of the server. Client can use it to connect to the server.

#### `server.clientSeed`

Default secret client seed. Client can use it for authentication.

#### `const db = new Protobee(serverKey, seed, [options])`

Creates a RPC client to connects to the server.

`serverKey` must be `server.key`.

`seed` must be `server.clientSeed`, it's used to generate the client key pair.

Available `options`:
```js
{
  keyEncoding, // It doesn't support custom codecs, only a string from codecs or a compact-encoding method
  valueEncoding,
  dht, // DHT instance
  bootstrap // Array of bootstrap nodes (only if you didn't pass a DHT instance)
}
```

#### The rest of the API

Practically the same API as Hyperbee:

https://github.com/holepunchto/hyperbee

## Warning

Differences with Hyperbee:

- The way you create the Protobee instance is different but this is expected.
- Errors are very different at the moment.
- Possible bugs around the `version` property due core truncates (needs more debugging and testing).
- In `bee.put` method the `cas` option is a bool, and if you pass a function it will throw.
- In `bee.del` method the `cas` option is not supported, and it will throw if you use it.

Notes:

- A bad client can create unlimited snapshots or sending bad requests (needs protection settings).

Current unsupported methods:

- `sub` (in favor of using sub-encoder lib, but it might end up being supported)
- `watch`
- `getAndWatch`

## License

MIT
