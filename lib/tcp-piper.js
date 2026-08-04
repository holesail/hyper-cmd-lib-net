const net = require('net')

function connPiper(a, bFactory, opts = {}) {
  const logger = opts.logger || {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {}
  }

  logger.info('Starting TCP connection piper')

  let b

  try {
    b = bFactory()
  } catch (err) {
    logger.error(`b factory error: ${err.message}`)
    a.destroy()
    return
  }

  if (b === null) {
    logger.warn('Connection rejected (null b)')
    a.destroy()
    return
  }

  let destroyed = false
  const destroy = (err) => {
    if (destroyed) return
    destroyed = true
    if (err) logger.info(`Destroying piper due to error: ${err.message}`)
    else logger.info('Destroying piper')
    a.destroy(err)
    b.destroy(err)
    opts.onDestroy?.(err)
  }

  a.pipe(b)
  b.pipe(a)

  a.on('error', destroy).on('close', () => destroy())
  b.on('error', destroy).on('close', () => destroy())
  a.on('data', (d) => logger.debug(`a→b: ${d.length} bytes`))
  b.on('data', (d) => logger.debug(`b→a: ${d.length} bytes`))
  a.on('end', () => logger.debug('a ended (peer-a sent FIN); pipe will end b'))
  b.on('end', () => logger.debug('b ended (peer-b sent FIN); pipe will end a'))
  b.on('connect', () => logger.info('Connected'))
}

function createTcpProxy(remoteTunnel, opts, onListen) {
  const sockets = new Set()

  const proxy = net.createServer({ allowHalfOpen: true }, (localStream) => {
    localStream.setNoDelay(true)
    sockets.add(localStream)
    localStream.on('close', () => sockets.delete(localStream))
    connPiper(localStream, remoteTunnel, opts)
  })
  proxy.listen(opts.port, opts.host, onListen)

  const close = proxy.close.bind(proxy)
  proxy.close = (cb) => {
    for (const sock of sockets) sock.destroy()
    return close(cb)
  }

  return proxy
}

function pipeTcpServer(remoteStream, leftover, opts) {
  const sock = net.connect({
    port: +opts.port,
    host: opts.host,
    allowHalfOpen: true
  })
  sock.setNoDelay(true)
  if (leftover && leftover.length) sock.write(leftover)
  return connPiper(remoteStream, () => sock, opts)
}

module.exports = { connPiper, createTcpProxy, pipeTcpServer }
