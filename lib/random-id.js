module.exports = function randomId (onid) {
  let id

  while (true) {
    id = (1 + Math.random() * (0x100000000 - 1)) >>> 0
    if (onid(id)) continue
    break
  }

  return id
}
