const test = require('brittle')
const net = require('net')
const { connPiper } = require('../lib/tcp-piper.js')
const { createLogger, tick, listenTcp, tcpPair, closePair, readOnce } = require('./helpers.js')

test('connPiper - pipes data from a to b', async function (t) {
  const A = await tcpPair()
  const B = await tcpPair()
  t.teardown(() => {
    closePair(A)
    closePair(B)
  })

  connPiper(A.local, () => B.local)
  const received = readOnce(B.remote)
  A.remote.write('hello-a-to-b')

  t.is(await received, 'hello-a-to-b')
})

test('connPiper - pipes data from b to a', async function (t) {
  const A = await tcpPair()
  const B = await tcpPair()
  t.teardown(() => {
    closePair(A)
    closePair(B)
  })

  connPiper(A.local, () => B.local)
  const received = readOnce(A.remote)
  B.remote.write('hello-b-to-a')

  t.is(await received, 'hello-b-to-a')
})

test('connPiper - bFactory throwing destroys a and logs the error', async function (t) {
  const A = await tcpPair()
  t.teardown(() => closePair(A))
  const { logger, calls } = createLogger()

  connPiper(
    A.local,
    () => {
      throw new Error('boom')
    },
    { logger }
  )
  await tick()

  t.ok(A.local.destroyed)
  t.is(calls.error.length, 1)
  t.ok(/b factory error: boom/.test(calls.error[0][0]))
})

test('connPiper - bFactory returning null destroys a and logs a warning', async function (t) {
  const A = await tcpPair()
  t.teardown(() => closePair(A))
  const { logger, calls } = createLogger()

  connPiper(A.local, () => null, { logger })
  await tick()

  t.ok(A.local.destroyed)
  t.is(calls.warn.length, 1)
  t.ok(/Connection rejected \(null b\)/.test(calls.warn[0][0]))
})

test('connPiper - works with the default no-op logger', async function (t) {
  const A = await tcpPair()
  t.teardown(() => closePair(A))

  connPiper(A.local, () => null)
  await tick()
  t.ok(A.local.destroyed)
})

test('connPiper - a erroring destroys b exactly once and notifies onDestroy', async function (t) {
  const A = await tcpPair()
  const B = await tcpPair()
  t.teardown(() => {
    closePair(A)
    closePair(B)
  })
  const destroys = []

  connPiper(A.local, () => B.local, { onDestroy: (err) => destroys.push(err) })
  A.local.destroy(new Error('kaboom'))
  await tick()

  t.ok(B.local.destroyed)
  t.is(destroys.length, 1)
  t.is(destroys[0].message, 'kaboom')
})

test('connPiper - b erroring destroys a exactly once and notifies onDestroy', async function (t) {
  const A = await tcpPair()
  const B = await tcpPair()
  t.teardown(() => {
    closePair(A)
    closePair(B)
  })
  const destroys = []

  connPiper(A.local, () => B.local, { onDestroy: (err) => destroys.push(err) })
  B.local.destroy(new Error('kaboom-b'))
  await tick()

  t.ok(A.local.destroyed)
  t.is(destroys.length, 1)
  t.is(destroys[0].message, 'kaboom-b')
})

test('connPiper - destroy is idempotent across both error and close events', async function (t) {
  const A = await tcpPair()
  const B = await tcpPair()
  t.teardown(() => {
    closePair(A)
    closePair(B)
  })
  const destroys = []

  connPiper(A.local, () => B.local, { onDestroy: (err) => destroys.push(err) })
  // destroy(err) on a real socket emits both 'error' and 'close', and both are
  // wired to the same internal destroy() closure — this alone exercises the guard.
  A.local.destroy(new Error('once-only'))
  await tick()

  t.is(destroys.length, 1)
})

test('connPiper - logs debug messages describing byte counts in both directions', async function (t) {
  const A = await tcpPair()
  const B = await tcpPair()
  t.teardown(() => {
    closePair(A)
    closePair(B)
  })
  const { logger, calls } = createLogger()

  connPiper(A.local, () => B.local, { logger })
  A.remote.write('abc')
  B.remote.write('de')
  await tick()

  t.ok(calls.debug.some((args) => /a.b: 3 bytes/.test(args[0])))
  t.ok(calls.debug.some((args) => /b.a: 2 bytes/.test(args[0])))
})

test('connPiper - logs Connected when b emits a connect event', async function (t) {
  const A = await tcpPair()
  t.teardown(() => closePair(A))

  const bServer = net.createServer({ allowHalfOpen: true })
  t.teardown(() => bServer.close())
  const addr = await listenTcp(bServer)

  const { logger, calls } = createLogger()
  const b = net.connect(addr.port, '127.0.0.1')
  t.teardown(() => b.destroy())
  // net.connect() hasn't fired 'connect' yet here — connPiper's own listener
  // attaches before that happens, same as pipeTcpServer does in production.
  connPiper(A.local, () => b, { logger })

  await new Promise((resolve) => bServer.once('connection', resolve))
  await tick()

  t.ok(calls.info.some((args) => args[0] === 'Connected'))
})

test('connPiper - a ending its readable side ends b in turn', async function (t) {
  const A = await tcpPair()
  const B = await tcpPair()
  t.teardown(() => {
    closePair(A)
    closePair(B)
  })
  const { logger, calls } = createLogger()

  connPiper(A.local, () => B.local, { logger })
  const bFinished = new Promise((resolve) => B.local.once('finish', resolve))
  A.remote.end()
  await bFinished

  t.ok(calls.debug.some((args) => /a ended/.test(args[0])))
})
