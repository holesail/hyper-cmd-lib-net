const dgram = require('bare-dgram')

const DEFAULT_MAX_FRAME_SIZE = 65535

function createUdpFramedProxy(createTunnel, opts = {}, onBind) {
  const port = opts.port
  const host = opts.host
  const logger = opts.logger || {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {}
  }
  const maxFrameSize = opts.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE

  const proxySocket = dgram.createSocket('udp4')
  const clients = new Map()

  proxySocket.on('error', (err) => {
    logger.error(`Proxy socket error: ${err.stack}`)
    proxySocket.close()
  })

  proxySocket.on('message', (msg, rinfo) => {
    const clientId = `${rinfo.address}:${rinfo.port}`
    logger.debug(`UDP message from ${clientId}: ${msg.length} bytes`)
    let client = clients.get(clientId)
    if (!client) {
      const stream = createTunnel()
      client = { stream, rinfo, buffer: Buffer.alloc(0) }
      clients.set(clientId, client)
      stream.on('data', (chunk) => {
        client.buffer = Buffer.concat([client.buffer, chunk])
        while (client.buffer.length >= 4) {
          const len = client.buffer.readUInt32BE(0)
          if (len > maxFrameSize) {
            logger.error(`Frame too large from ${clientId}: ${len} > ${maxFrameSize}`)
            clients.delete(clientId)
            stream.destroy(new Error(`Frame too large: ${len}`))
            return
          }
          if (client.buffer.length < 4 + len) break
          const response = client.buffer.subarray(4, 4 + len)
          logger.debug(`UDP response for ${clientId}: ${response.length} bytes`)
          proxySocket.send(response, 0, response.length, rinfo.port, rinfo.address, (err) => {
            if (err) logger.error(`Socket error while sending packet to ${clientId}: ${err.stack}`)
          })
          client.buffer = client.buffer.subarray(4 + len)
        }
      })
      stream.on('error', (err) => {
        logger.error(`Remote error for ${clientId}: ${err.stack}`)
        clients.delete(clientId)
        stream.destroy()
      })
      stream.on('close', () => {
        logger.debug(`Remote close for ${clientId}`)
        clients.delete(clientId)
      })
    }
    const lenBuf = Buffer.alloc(4)
    lenBuf.writeUInt32BE(msg.length, 0)
    client.stream.write(Buffer.concat([lenBuf, msg]))
  })

  proxySocket.bind(port, host, onBind)

  return { proxySocket, clients }
}

function pipeUdpFramedServer(stream, leftover, opts = {}) {
  const port = opts.port
  const host = opts.host
  const logger = opts.logger || {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {}
  }
  const maxFrameSize = opts.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE

  const localSocket = dgram.createSocket('udp4')
  let buffer = leftover && leftover.length ? Buffer.from(leftover) : Buffer.alloc(0)

  let destroyed = false
  const cleanup = (err) => {
    if (destroyed) return
    destroyed = true
    if (err) logger.info(`Destroying pipe due to error: ${err.message}`)
    else logger.info('Destroying pipe')
    try {
      localSocket.close()
    } catch {}
    if (err) stream.destroy(err)
    else stream.end()
  }

  const drain = () => {
    while (buffer.length >= 4) {
      const len = buffer.readUInt32BE(0)
      if (len > maxFrameSize) {
        cleanup(new Error(`Frame too large: ${len} > ${maxFrameSize}`))
        return
      }
      if (buffer.length < 4 + len) break
      const msg = buffer.subarray(4, 4 + len)
      localSocket.send(msg, 0, msg.length, port, host, (err) => {
        if (err) logger.error(`Error sending packet to local socket: ${err.stack}`)
      })
      buffer = buffer.subarray(4 + len)
    }
  }

  localSocket.on('error', (err) => {
    logger.error(`Local UDP socket error: ${err.stack}`)
    cleanup(err)
  })
  localSocket.on('message', (msg) => {
    logger.debug(`Data from local to remote: ${msg.length} bytes`)
    const lenBuf = Buffer.alloc(4)
    lenBuf.writeUInt32BE(msg.length, 0)
    stream.write(Buffer.concat([lenBuf, msg]))
  })
  localSocket.on('close', () => cleanup())
  stream.on('data', (chunk) => {
    logger.debug(`Data from remote to local: ${chunk.length} bytes`)
    buffer = Buffer.concat([buffer, chunk])
    drain()
  })
  stream.on('end', () => cleanup())
  stream.on('close', () => cleanup())
  stream.on('error', (err) => cleanup(err))

  if (buffer.length) drain()
}

module.exports = { createUdpFramedProxy, pipeUdpFramedServer }
