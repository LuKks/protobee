const ProtobeeServer = require('./protobee-server.js')
const Protobee = require('./protobee-client.js')

module.exports = function (input, opts) {
  if (input.core) return new ProtobeeServer(input, opts)
  return new Protobee(input, opts)
}
