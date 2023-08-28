const test = require('brittle')
const { create } = require('./helpers/index.js')

test('batch', async function (t) {
  const { db } = await create(t)

  await db.put('/a', '1')

  const batch = db.batch()
  t.is(batch.version, 2)

  await batch.lock()

  const put = db.put('/b', '2')

  t.is(await batch.get('/b'), null)
  t.is(batch.version, 2)

  await batch.put('/b', '3')
  t.is(batch.version, 3)
  t.alike(await batch.get('/b'), { seq: 2, key: '/b', value: '3' })

  t.is(db.version, 2)
  await batch.flush()
  t.is(db.version, 3)

  await put
  t.is(db.version, 4)
  t.alike(await db.get('/b'), { seq: 3, key: '/b', value: '2' })
})

test('batch flush vs close', async function (t) {
  const { db } = await create(t)

  const a = db.batch()
  await a.put('/a', '1')
  await a.flush()

  t.alike(await db.get('/a'), { seq: 1, key: '/a', value: '1' })

  const b = db.batch()
  await b.put('/b', '2')
  await b.close()

  t.alike(await db.get('/b'), null)

  t.is(db.version, 2)
})

test('batch without ready', async function (t) {
  const { db } = await create(t, { data: true, ready: false })

  const b = db.batch()

  t.alike(await b.get('/test'), { seq: 1, key: '/test', value: 'abc' })

  await b.close()
})
