const dgram = require('bare-dgram')
const net = require('net')
const EventEmitter = require('events')

function connPiper(connection, _dst, opts = {}, stats = {}) {
  const logger = opts.logger || {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {}
  }
  logger.info('Starting TCP connection piper')
  const loc = _dst()
  if (loc === null) {
    logger.warn('Connection rejected (null destination)')
    connection.destroy() // don't return rejection error
    if (!stats.rejectCnt) {
      stats.rejectCnt = 0
    }
    stats.rejectCnt++
    return
  }
  if (!stats.locCnt) {
    stats.locCnt = 0
  }
  if (!stats.remCnt) {
    stats.remCnt = 0
  }
  stats.locCnt++
  stats.remCnt++
  let destroyed = false

  // loc is the remote stream experimentation here
  loc.on('data', (d) => {
    logger.debug(`Data from local to remote: ${d.length} bytes`)
    connection.write(d)
  })
  connection.on('data', (d) => {
    logger.debug(`Data from remote to local: ${d.length} bytes`)
    loc.write(d)
  })
  loc.on('error', destroy).on('close', destroy)
  connection.on('error', destroy).on('close', destroy)
  loc.on('end', () => {
    logger.debug('Local end, ending connection')
    connection.end()
  })
  connection.on('end', () => {
    logger.debug('Connection end, ending local')
    loc.end()
  })
  loc.on('connect', (err) => {
    if (err) logger.error(err.message)
    else logger.info('Connected')
  })
  function destroy(err) {
    if (destroyed) return
    if (err) logger.info(`Destroying connection piper due to error: ${err.message}`)
    else logger.info('Destroying connection piper')
    stats.locCnt--
    stats.remCnt--
    destroyed = true
    loc.end()
    connection.end()
    loc.destroy(err)
    connection.destroy(err)
    if (opts.onDestroy) {
      opts.onDestroy(err)
    }
  }
  return {}
}

class UdpSocket {
  constructor(opts) {
    this.opts = opts
    this.logger = this.opts.logger || { log: () => {} }
    this.server = dgram.createSocket('udp4')
    this.client = dgram.createSocket('udp4')
    this.event = new EventEmitter()
    this.rinfo = null
    this.connect()
    if (opts.bind) this.logger.info(`UDP socket created, binding to ${opts.host}:${opts.port}`)
    else this.logger.info('UDP socket created')
  }

  connect() {
    if (this.opts.bind) {
      this.server.bind(this.opts.port, this.opts.host)
    }
    this.server.on('message', (msg, rinfo) => {
      this.logger.debug(
        `UDP message from server: ${msg.length} bytes from ${rinfo.address}:${rinfo.port}`
      )
      this.event.emit('message', msg, rinfo)
      this.rinfo = rinfo
    })
    this.client.on('message', (response, rinfo) => {
      this.logger.debug(
        `UDP message from client: ${response.length} bytes from ${rinfo.address}:${rinfo.port}`
      )
      this.event.emit('message', response)
    })
    this.client.on('error', (err) => {
      this.logger.error(`UDP error: ${err.stack}`)
      this.client.close()
    })
  }

  write(msg) {
    this.logger.debug(`Writing UDP message: ${msg.length} bytes`)
    if (this.rinfo) {
      this.server.send(msg, 0, msg.length, this.rinfo.port, this.rinfo.address)
    } else {
      this.client.send(msg, 0, msg.length, this.opts.port, this.opts.host)
    }
  }
}

class UdpConnPiper {
  constructor(remote, local, opts = {}) {
    this.opts = opts
    this.logger = this.opts.logger || { log: () => {} }
    this.remote = remote
    this.local = local
    this.client = opts.client
    this.retryDelay = opts.retryDelay || 2000
    this.destroyed = false
    this._bindListeners()
    this.connect()
    if (opts.client) this.logger.info('Starting UDP connection piper (client mode)')
    else this.logger.info('Starting UDP connection piper (server mode)')
  }

  _bindListeners() {
    this.bound = {
      onLocMessage: this.onLocMessage.bind(this),
      onConnectionMessage: this.onConnectionMessage.bind(this),
      onLocError: this._handleError.bind(this),
      onLocClose: this._handleError.bind(this),
      onConnectionError: this._handleError.bind(this),
      onConnectionClose: this._handleError.bind(this)
    }
  }

  connect() {
    if (this.destroyed) return
    this.logger.debug('Connecting UDP piper')
    this.removeListeners()
    this.localStream = typeof this.local === 'function' ? this.local() : this.local
    this.remoteStream = typeof this.remote === 'function' ? this.remote() : this.remote
    if (!this.localStream || !this.remoteStream) {
      this.logger.warn('UDP connect failed (missing streams)')
      this.destroy()
      return
    }
    this.attachListeners()
  }

  attachListeners() {
    this.localStream.event.on('message', this.bound.onLocMessage)
    this.localStream.server.on('error', this.bound.onLocError)
    this.localStream.server.on('close', this.bound.onLocClose)
    this.remoteStream.on('message', this.bound.onConnectionMessage)
    this.remoteStream.on('error', this.bound.onConnectionError)
    this.remoteStream.on('close', this.bound.onConnectionClose)
    this.logger.debug('UDP listeners attached')
  }

  removeListeners() {
    if (this.localStream) {
      this.localStream.event.off('message', this.bound.onLocMessage)
      this.localStream.server.off('error', this.bound.onLocError)
      this.localStream.server.off('close', this.bound.onLocClose)
    }
    if (this.remoteStream) {
      this.remoteStream.off('message', this.bound.onConnectionMessage)
      this.remoteStream.off('error', this.bound.onConnectionError)
      this.remoteStream.off('close', this.bound.onConnectionClose)
    }
    this.logger.debug('UDP listeners removed')
  }

  onLocMessage(msg, rinfo) {
    this.logger.debug(`UDP message from local: ${msg.length} bytes`)
    if (this.remoteStream && !this.destroyed) {
      this.remoteStream.trySend?.(msg)
    }
  }

  onConnectionMessage(msg) {
    this.logger.debug(`UDP message from connection: ${msg.length} bytes`)
    if (this.localStream && !this.destroyed) {
      this.localStream.write?.(msg)
    }
  }

  _handleError(err) {
    this.logger.warn(`UDP error: ${err ? err.message : 'close'}`)
    this.destroy(err)
  }

  destroy(err) {
    if (this.destroyed) return
    this.destroyed = true
    if (err) this.logger.info(`Destroying UDP piper due to error: ${err.message}`)
    else this.logger.info('Destroying UDP piper')
    this.removeListeners()
    try {
      this.localStream?.destroy?.(err)
    } catch (e) {}
    try {
      this.remoteStream?.close?.(err)
    } catch (e) {}
    if (this.client) {
      this.logger.info(`Scheduling retry in ${this.retryDelay}ms`)
      setTimeout(() => {
        this.destroyed = false
        this.connect()
      }, this.retryDelay)
    }
  }
}

function udpConnect(opts, callback) {
  const socket = new UdpSocket(opts)
  if (typeof callback === 'function') {
    callback(socket)
  } else {
    return socket
  }
}

function udpPiper(connection, _dst, opts) {
  return new UdpConnPiper(connection, _dst, opts)
}

function createTcpProxy(listenOpts, connectRemote, piperOpts, stats, onListen) {
  const proxy = net.createServer({ allowHalfOpen: true }, (c) => {
    connPiper(c, connectRemote, piperOpts, stats)
  })
  proxy.listen(listenOpts.port, listenOpts.host, onListen)
  return proxy
}

function pipeTcpServer(remoteStream, localOpts, piperOpts, stats) {
  connPiper(
    remoteStream,
    () =>
      net.connect({
        port: +localOpts.port,
        host: localOpts.host,
        allowHalfOpen: true
      }),
    piperOpts,
    stats
  )
}

function createUdpFramedProxy(listenOpts, connectRemote, logger, onBind) {
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
      const remoteStream = connectRemote()
      client = { remoteStream, rinfo, buffer: Buffer.alloc(0) }
      clients.set(clientId, client)
      remoteStream.on('data', (chunk) => {
        client.buffer = Buffer.concat([client.buffer, chunk])
        while (client.buffer.length >= 4) {
          const len = client.buffer.readUInt32BE(0)
          if (client.buffer.length < 4 + len) break
          const response = client.buffer.slice(4, 4 + len)
          logger.debug(`UDP response for ${clientId}: ${response.length} bytes`)
          proxySocket.send(response, 0, response.length, rinfo.port, rinfo.address, (err) => {
            if (err) logger.error(`Send error to ${clientId}: ${err.stack}`)
          })
          client.buffer = client.buffer.slice(4 + len)
        }
      })
      remoteStream.on('error', (err) => {
        logger.error(`Remote error for ${clientId}: ${err.stack}`)
        clients.delete(clientId)
        remoteStream.destroy()
      })
      remoteStream.on('close', () => {
        logger.debug(`Remote close for ${clientId}`)
        clients.delete(clientId)
      })
    }
    const lenBuf = Buffer.alloc(4)
    lenBuf.writeUInt32BE(msg.length, 0)
    client.remoteStream.write(Buffer.concat([lenBuf, msg]))
  })

  proxySocket.bind(listenOpts.port, listenOpts.host, onBind)

  return { proxySocket, clients }
}

function pipeUdpFramedServer(remoteStream, localOpts, logger, stats) {
  const localSocket = dgram.createSocket('udp4')
  let buffer = Buffer.alloc(0)
  localSocket.on('error', (err) => {
    logger.error(`Local UDP socket error: ${err.stack}`)
    remoteStream.destroy(err)
  })
  localSocket.on('message', (msg) => {
    logger.debug(`Data from local to remote: ${msg.length} bytes`)
    const lenBuf = Buffer.alloc(4)
    lenBuf.writeUInt32BE(msg.length, 0)
    remoteStream.write(Buffer.concat([lenBuf, msg]))
  })
  remoteStream.on('data', (chunk) => {
    logger.debug(`Data from remote to local: ${chunk.length} bytes`)
    buffer = Buffer.concat([buffer, chunk])
    while (buffer.length >= 4) {
      const len = buffer.readUInt32BE(0)
      if (buffer.length < 4 + len) break
      const msg = buffer.slice(4, 4 + len)
      localSocket.send(msg, 0, msg.length, +localOpts.port, localOpts.host, (err) => {
        if (err) logger.error(`Send error to local: ${err.stack}`)
      })
      buffer = buffer.slice(4 + len)
    }
  })
  remoteStream.on('end', () => localSocket.close())
  localSocket.on('close', () => remoteStream.end())
  remoteStream.on('error', (err) => localSocket.close())
  localSocket.on('error', (err) => remoteStream.destroy(err))
}

module.exports = {
  connPiper,
  udpPiper,
  udpConnect,
  createTcpProxy,
  pipeTcpServer,
  createUdpFramedProxy,
  pipeUdpFramedServer
}
