const test = require('brittle')
const net = require('net')
const { pipeTcpServer } = require('../lib/tcp-piper.js')
const { fakeSocket, tick, listenTcp } = require('./helpers.js')

test('pipeTcpServer - writes leftover bytes to the local connection before piping', async function (t) {
  const echoServer = net.createServer((sock) => sock.pipe(sock))
  t.teardown(() => echoServer.close())
  const addr = await listenTcp(echoServer)

  const remoteStream = fakeSocket()
  t.teardown(() => remoteStream.destroy())
  pipeTcpServer(remoteStream, Buffer.from('LEFTOVER'), { port: addr.port, host: '127.0.0.1' })
  await tick(30)

  t.alike(remoteStream.written, [Buffer.from('LEFTOVER')])
})

test('pipeTcpServer - accepts a string port and pipes further data after the leftover', async function (t) {
  const echoServer = net.createServer((sock) => sock.pipe(sock))
  t.teardown(() => echoServer.close())
  const addr = await listenTcp(echoServer)

  const remoteStream = fakeSocket()
  t.teardown(() => remoteStream.destroy())
  pipeTcpServer(remoteStream, Buffer.from('LEFTOVER'), {
    port: String(addr.port),
    host: '127.0.0.1'
  })
  await tick(30)
  remoteStream.push(Buffer.from('MORE-DATA'))
  await tick(30)

  t.alike(remoteStream.written, [Buffer.from('LEFTOVER'), Buffer.from('MORE-DATA')])
})

test('pipeTcpServer - works with no leftover at all', async function (t) {
  const echoServer = net.createServer((sock) => sock.pipe(sock))
  t.teardown(() => echoServer.close())
  const addr = await listenTcp(echoServer)

  const remoteStream = fakeSocket()
  t.teardown(() => remoteStream.destroy())
  pipeTcpServer(remoteStream, null, { port: addr.port, host: '127.0.0.1' })
  await tick(20)
  t.is(remoteStream.written.length, 0, 'nothing sent yet without leftover or data')

  remoteStream.push(Buffer.from('first-word'))
  await tick(30)
  t.alike(remoteStream.written, [Buffer.from('first-word')])
})

test('pipeTcpServer - a refused TCP connection destroys the remote stream', async function (t) {
  // Bind a server, close it immediately, then connect: the port is free but
  // nothing is listening, so the connect attempt fails with ECONNREFUSED.
  const throwaway = net.createServer(() => {})
  const addr = await listenTcp(throwaway)
  await new Promise((resolve) => throwaway.close(resolve))

  const remoteStream = fakeSocket()
  const closed = new Promise((resolve) => remoteStream.once('close', resolve))
  pipeTcpServer(remoteStream, null, { port: addr.port, host: '127.0.0.1' })
  await closed

  t.ok(remoteStream.destroyed)
})
