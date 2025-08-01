const dgram = require('bare-dgram')
const EventEmitter = require('events')

function connPiper (connection, _dst, opts = {}, stats = {}) {
  const log = (msg) => {
    if (opts.logger) opts.logger.log(msg)
  }
  const logError = (msg) => {
    if (opts.logger) opts.logger.error(msg)
  }
  log('Starting TCP connPiper')
  const loc = _dst()
  if (loc === null) {
    log('Connection rejected (null destination)')
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
  loc.on('data', d => {
    log(`Data from local to remote: ${d.length} bytes`)
    connection.write(d)
  })
  connection.on('data', d => {
    log(`Data from remote to local: ${d.length} bytes`)
    loc.write(d)
  })
  loc.on('error', destroy).on('close', destroy)
  connection.on('error', destroy).on('close', destroy)
  loc.on('end', () => {
    log('Local end, ending connection')
    connection.end()
  })
  connection.on('end', () => {
    log('Connection end, ending local')
    loc.end()
  })
  loc.on('connect', err => {
    if (opts.debug) {
      console.log('connected')
    }
    if (err) {
      logError(err.message)
    } else {
      log('Connected')
    }
  })
  function destroy (err) {
    if (destroyed) {
      return
    }
    log(`Destroying connPiper${err ? ` due to error: ${err.message}` : ''}`)
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
  constructor (opts) {
    this.opts = opts
    this.server = dgram.createSocket('udp4')
    this.client = dgram.createSocket('udp4')
    this.event = new EventEmitter()
    this.rinfo = null
    this.log = (msg) => {
      if (this.opts.logger) this.opts.logger.log(msg)
    }
    this.logError = (msg) => {
      if (this.opts.logger) this.opts.logger.error(msg)
    }
    this.connect()
    this.log(`UDP socket created${opts.bind ? `, binding to ${opts.host}:${opts.port}` : ''}`)
  }

  connect () {
    if (this.opts.bind) {
      this.server.bind(this.opts.port, this.opts.host)
    }
    this.server.on('message', (msg, rinfo) => {
      this.log(`UDP message from server: ${msg.length} bytes from ${rinfo.address}:${rinfo.port}`)
      this.event.emit('message', msg, rinfo)
      this.rinfo = rinfo
    })
    this.client.on('message', (response, rinfo) => {
      this.log(`UDP message from client: ${response.length} bytes from ${rinfo.address}:${rinfo.port}`)
      this.event.emit('message', response)
    })
    this.client.on('error', (err) => {
      this.logError(`UDP error: ${err.stack}`)
      this.client.close()
    })
  }

  write (msg) {
    this.log(`Writing UDP message: ${msg.length} bytes`)
    if (this.rinfo) {
      this.server.send(msg, 0, msg.length, this.rinfo.port, this.rinfo.address)
    } else {
      this.client.send(msg, 0, msg.length, this.opts.port, this.opts.host)
    }
  }
}

class UdpConnPiper {
  constructor (remote, local, opts = {}) {
    this.opts = opts
    this.remote = remote
    this.local = local
    this.client = opts.client
    this.debug = opts.debug || false
    this.retryDelay = opts.retryDelay || 2000
    this.destroyed = false
    this.log = (msg) => {
      if (this.opts.logger) this.opts.logger.log(msg)
    }
    this._bindListeners()
    this.connect()
    this.log(`Starting UDP piper${opts.client ? ' (client mode)' : ' (server mode)'}`)
  }

  _bindListeners () {
    this.bound = {
      onLocMessage: this.onLocMessage.bind(this),
      onConnectionMessage: this.onConnectionMessage.bind(this),
      onLocError: this._handleError.bind(this),
      onLocClose: this._handleError.bind(this),
      onConnectionError: this._handleError.bind(this),
      onConnectionClose: this._handleError.bind(this)
    }
  }

  connect () {
    if (this.destroyed) return
    this.log('Connecting UDP piper')
    this.removeListeners()
    this.localStream = typeof this.local === 'function' ? this.local() : this.local
    this.remoteStream = typeof this.remote === 'function' ? this.remote() : this.remote
    if (!this.localStream || !this.remoteStream) {
      this.log('UDP connect failed (missing streams)')
      this.destroy()
      return
    }
    this.attachListeners()
  }

  attachListeners () {
    this.localStream.event.on('message', this.bound.onLocMessage)
    this.localStream.server.on('error', this.bound.onLocError)
    this.localStream.server.on('close', this.bound.onLocClose)
    this.remoteStream.on('message', this.bound.onConnectionMessage)
    this.remoteStream.on('error', this.bound.onConnectionError)
    this.remoteStream.on('close', this.bound.onConnectionClose)
    this.log('UDP listeners attached')
  }

  removeListeners () {
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
    this.log('UDP listeners removed')
  }

  onLocMessage (msg, rinfo) {
    this.log(`UDP message from local: ${msg.length} bytes`)
    if (this.remoteStream && !this.destroyed) {
      this.remoteStream.trySend?.(msg)
    }
  }

  onConnectionMessage (msg) {
    this.log(`UDP message from connection: ${msg.length} bytes`)
    if (this.localStream && !this.destroyed) {
      this.localStream.write?.(msg)
    }
  }

  _handleError (err) {
    this.log(`UDP error: ${err ? err.message : 'close'}`)
    this.destroy(err)
  }

  destroy (err) {
    if (this.destroyed) return
    this.destroyed = true
    this.log(`Destroying UDP piper${err ? ` due to error: ${err.message}` : ''}`)
    this.removeListeners()
    try { this.localStream?.destroy?.(err) } catch (e) {}
    try { this.remoteStream?.close?.(err) } catch (e) {}
    if (this.client) {
      this.log(`Scheduling retry in ${this.retryDelay}ms`)
      setTimeout(() => {
        this.destroyed = false
        this.connect()
      }, this.retryDelay)
    }
  }
}

function udpConnect (opts, callback) {
  const socket = new UdpSocket(opts)
  if (typeof callback === 'function') {
    callback(socket)
  } else {
    return socket
  }
}

function udpPiper (connection, _dst, opts) {
  return new UdpConnPiper(connection, _dst, opts)
}

module.exports = {
  connPiper,
  udpPiper,
  udpConnect
}
