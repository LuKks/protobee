let id = 1

module.exports = function randomId (onused) {
  while (true) {
    if (id === 0xffffffff) id = 1

    const i = id++
    if (onused(i)) continue

    return i
  }
}
