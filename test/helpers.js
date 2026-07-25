const { Duplex } = require('streamx')
const net = require('net')
const dgram = require('bare-dgram')

function fakeSocket() {
  const written = []
  const s = new Duplex({
    read() {},
    write(chunk, cb) {
      written.push(chunk)
      cb(null)
    }
  })
  s.written = written
  return s
}

// A stream that echoes back whatever is written to it, simulating a remote
// peer that reflects data (used as the "far end" of a tunnel/proxy).
function echoStream() {
  return new Duplex({
    read() {},
    write(chunk, cb) {
      this.push(chunk)
      cb(null)
    }
  })
}

// Similar to echoStream(), but pushes each write back in two separate chunks
function splitEchoStream() {
  return new Duplex({
    read() {},
    write(chunk, cb) {
      const mid = Math.floor(chunk.length / 2) || 1
      this.push(chunk.subarray(0, mid))
      setImmediate(() => {
        this.push(chunk.subarray(mid))
        cb(null)
      })
    }
  })
}

function spyLogger() {
  const calls = { debug: [], info: [], warn: [], error: [] }
  const logger = {
    debug: (...a) => calls.debug.push(a),
    info: (...a) => calls.info.push(a),
    warn: (...a) => calls.warn.push(a),
    error: (...a) => calls.error.push(a)
  }
  return { logger, calls }
}

function frame(payload) {
  const len = Buffer.alloc(4)
  len.writeUInt32BE(payload.length, 0)
  return Buffer.concat([len, payload])
}

function tick(ms = 20) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function bindUdpClient() {
  return new Promise((resolve, reject) => {
    const sock = dgram.createSocket('udp4')
    sock.on('error', reject)
    sock.bind(0, '127.0.0.1', () => resolve(sock))
  })
}

function listenTcp(server) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address()))
  })
}

// Two real, independently connected loopback TCP sockets. `local` is the
// server-accepted side — what gets handed to connPiper as `a` or `b` — and
// `remote` is the test's own hook into "the other end" of that connection,
// used to inject inbound bytes (remote.write) and observe whatever connPiper
// forwarded out to it (remote.on('data', ...)).
function tcpPair() {
  return new Promise((resolve) => {
    const server = net.createServer({ allowHalfOpen: true })
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address()
      const remote = net.connect(addr.port, '127.0.0.1')
      server.once('connection', (local) => resolve({ local, remote, server }))
    })
  })
}

function closePair(pair) {
  pair.remote.destroy()
  pair.local.destroy()
  pair.server.close()
}

function readOnce(sock) {
  return new Promise((resolve) => sock.once('data', (d) => resolve(d.toString())))
}

async function withEchoUdpServer(fn) {
  const server = dgram.createSocket('udp4')
  server.on('message', (msg, rinfo) => {
    server.send(msg, 0, msg.length, rinfo.port, rinfo.address)
  })
  await new Promise((resolve) => server.bind(0, '127.0.0.1', resolve))
  try {
    await fn(server.address())
  } finally {
    server.close()
  }
}

module.exports = {
  fakeSocket,
  echoStream,
  splitEchoStream,
  spyLogger,
  frame,
  tick,
  bindUdpClient,
  listenTcp,
  tcpPair,
  closePair,
  readOnce,
  withEchoUdpServer
}
