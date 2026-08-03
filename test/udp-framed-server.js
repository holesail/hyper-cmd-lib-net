const test = require('brittle')
const { pipeUdpFramedServer } = require('../lib/udp-framed.js')
const { fakeSocket, createLogger, frame, tick, withEchoUdpServer } = require('./helpers.js')

test('pipeUdpFramedServer - forwards a framed message to the local service and frames the reply back', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    pipeUdpFramedServer(stream, null, { port: addr.port, host: '127.0.0.1' })

    const payload = Buffer.from('ping-udp-server')
    stream.push(frame(payload))
    await tick(40)

    t.is(stream.written.length, 1)
    const reply = stream.written[0]
    const len = reply.readUInt32BE(0)
    t.is(len, payload.length)
    t.alike(reply.subarray(4, 4 + len), payload)

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - drains a complete frame already present in leftover', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    const payload = Buffer.from('leftover-complete')
    pipeUdpFramedServer(stream, frame(payload), { port: addr.port, host: '127.0.0.1' })
    await tick(40)

    const reply = stream.written[0]
    const len = reply.readUInt32BE(0)
    t.alike(reply.subarray(4, 4 + len), payload)

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - reassembles a frame split across leftover and later stream data', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    const full = frame(Buffer.from('split-across-chunks'))
    const partial = full.subarray(0, 6) // 4-byte length prefix + 2 payload bytes
    const rest = full.subarray(6)

    pipeUdpFramedServer(stream, partial, { port: addr.port, host: '127.0.0.1' })
    await tick(20)
    t.is(stream.written.length, 0, 'incomplete frame is not forwarded yet')

    stream.push(rest)
    await tick(40)

    const reply = stream.written[0]
    const len = reply.readUInt32BE(0)
    t.alike(reply.subarray(4, 4 + len), Buffer.from('split-across-chunks'))

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - handles back-to-back frames delivered in a single chunk', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    pipeUdpFramedServer(stream, null, { port: addr.port, host: '127.0.0.1' })

    const combined = Buffer.concat([frame(Buffer.from('one')), frame(Buffer.from('two'))])
    stream.push(combined)
    await tick(40)

    const payloads = stream.written.map((buf) => {
      const len = buf.readUInt32BE(0)
      return buf.subarray(4, 4 + len).toString()
    })
    t.alike(payloads.sort(), ['one', 'two'])

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - a frame over maxFrameSize destroys the stream with a descriptive error', async function (t) {
  const stream = fakeSocket()
  let err
  stream.on('error', (e) => {
    err = e
  })

  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1', maxFrameSize: 4 })
  stream.push(frame(Buffer.from('too-big-payload')))
  await tick(30)

  t.ok(stream.destroyed)
  t.ok(err && /Frame too large: 15 > 4/.test(err.message))
})

test('pipeUdpFramedServer - a remote stream error destroys the pipe', async function (t) {
  const stream = fakeSocket()
  const { logger, calls } = createLogger()
  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1', logger })

  const closed = new Promise((resolve) => stream.once('close', resolve))
  stream.emit('error', new Error('peer blew up'))
  await closed

  t.ok(stream.destroyed)
  t.ok(calls.info.some((args) => /Destroying pipe due to error: peer blew up/.test(args[0])))
})

test('pipeUdpFramedServer - ends its writable side (no error) when the remote stream ends', async function (t) {
  const stream = fakeSocket()
  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1' })

  const finished = new Promise((resolve) => stream.once('finish', resolve))
  stream.emit('end')
  await finished

  t.absent(stream.destroyed, 'a clean end() does not destroy the stream')
})

test('pipeUdpFramedServer - cleanup only runs once no matter how many teardown events fire', async function (t) {
  const stream = fakeSocket()
  const { logger, calls } = createLogger()
  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1', logger })

  stream.emit('close')
  stream.emit('close')
  stream.emit('end')
  await tick()

  t.is(calls.info.filter((args) => /Destroying pipe/.test(args[0])).length, 1)
})

test('pipeUdpFramedServer - works with the default no-op logger', async function (t) {
  const stream = fakeSocket()
  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1' })

  const finished = new Promise((resolve) => stream.once('finish', resolve))
  stream.emit('end')
  await finished

  t.absent(stream.destroyed)
})
