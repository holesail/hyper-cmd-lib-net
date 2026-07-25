const test = require('brittle')
const net = require('net')
const { createTcpProxy } = require('../lib/tcp-piper.js')
const { echoStream, tick, listenTcp } = require('./helpers.js')

test('createTcpProxy - proxies a real TCP client round trip through the tunnel factory', async function (t) {
  const addr = await new Promise((resolve) => {
    const proxy = createTcpProxy(
      () => echoStream(),
      { port: 0, host: '127.0.0.1' },
      () => resolve(proxy.address())
    )
    t.teardown(() => proxy.close())
  })

  const client = net.connect(addr.port, '127.0.0.1')
  t.teardown(() => client.destroy())
  await new Promise((resolve) => client.once('connect', resolve))

  const reply = new Promise((resolve) => client.once('data', (d) => resolve(d.toString())))
  client.write('ping-through-proxy')

  t.is(await reply, 'ping-through-proxy')
})

test('createTcpProxy - calls onListen once the server is bound', async function (t) {
  let called = false
  const proxy = createTcpProxy(
    () => echoStream(),
    { port: 0, host: '127.0.0.1' },
    () => {
      called = true
    }
  )
  t.teardown(() => proxy.close())

  await new Promise((resolve) => proxy.once('listening', resolve))
  t.ok(called)
})

test('createTcpProxy - invokes the tunnel factory once per incoming connection', async function (t) {
  let calls = 0
  const proxy = createTcpProxy(
    () => {
      calls++
      return echoStream()
    },
    { port: 0, host: '127.0.0.1' },
    () => {}
  )
  t.teardown(() => proxy.close())

  const addr = await listenTcp(proxy)
  const c1 = net.connect(addr.port, '127.0.0.1')
  const c2 = net.connect(addr.port, '127.0.0.1')
  t.teardown(() => {
    c1.destroy()
    c2.destroy()
  })
  await Promise.all([
    new Promise((resolve) => c1.once('connect', resolve)),
    new Promise((resolve) => c2.once('connect', resolve))
  ])
  await tick()

  t.is(calls, 2)
})

test('createTcpProxy - two connections stay isolated from each other', async function (t) {
  const proxy = createTcpProxy(
    () => echoStream(),
    { port: 0, host: '127.0.0.1' },
    () => {}
  )
  t.teardown(() => proxy.close())

  const addr = await listenTcp(proxy)
  const c1 = net.connect(addr.port, '127.0.0.1')
  const c2 = net.connect(addr.port, '127.0.0.1')
  t.teardown(() => {
    c1.destroy()
    c2.destroy()
  })
  await Promise.all([
    new Promise((resolve) => c1.once('connect', resolve)),
    new Promise((resolve) => c2.once('connect', resolve))
  ])

  const r1 = new Promise((resolve) => c1.once('data', (d) => resolve(d.toString())))
  const r2 = new Promise((resolve) => c2.once('data', (d) => resolve(d.toString())))
  c1.write('from-c1')
  c2.write('from-c2')

  t.is(await r1, 'from-c1')
  t.is(await r2, 'from-c2')
})

test('createTcpProxy - closing the proxy destroys any still-open connections', async function (t) {
  const proxy = createTcpProxy(
    () => echoStream(),
    { port: 0, host: '127.0.0.1' },
    () => {}
  )

  const addr = await listenTcp(proxy)
  const client = net.connect(addr.port, '127.0.0.1')
  client.on('error', () => {}) // the far end destroys mid-flight; ECONNRESET is expected here
  t.teardown(() => client.destroy())
  await new Promise((resolve) => client.once('connect', resolve))

  const closed = new Promise((resolve) => client.once('close', resolve))
  await new Promise((resolve) => proxy.close(resolve))
  await closed

  t.pass('client socket was closed by proxy.close()')
})
