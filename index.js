const dgram = require('bare-dgram')
const EventEmitter = require('events')

function connPiper (connection, _dst, opts = {}, stats = {}) {
  if (opts.log) console.log(`${new Date().toISOString()} [LibNet] Starting TCP connPiper`)
  const loc = _dst()
  if (loc === null) {
    if (opts.log) console.log(`${new Date().toISOString()} [LibNet] Connection rejected (null destination)`)
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
    if (opts.log) console.log(`${new Date().toISOString()} [LibNet] Data from local to remote: ${d.length} bytes`)
    connection.write(d)
  })
  connection.on('data', d => {
    if (opts.log) console.log(`${new Date().toISOString()} [LibNet] Data from remote to local: ${d.length} bytes`)
    loc.write(d)
  })
  loc.on('error', destroy).on('close', destroy)
  connection.on('error', destroy).on('close', destroy)
  loc.on('end', () => {
    if (opts.log) console.log(`${new Date().toISOString()} [LibNet] Local end, ending connection`)
    connection.end()
  })
  connection.on('end', () => {
    if (opts.log) console.log(`${new Date().toISOString()} [LibNet] Connection end, ending local`)
    loc.end()
  })
  loc.on('connect', err => {
    if (opts.log) {
      if (err) {
        console.log(`${new Date().toISOString()} [LibNet] Connect error: ${err.message}`)
      } else {
        console.log(`${new Date().toISOString()} [LibNet] Connected`)
      }
    }
  })
  function destroy (err) {
    if (destroyed) {
      return
    }
    if (opts.log) console.log(`${new Date().toISOString()} [LibNet] Destroying connPiper${err ? ` due to error: ${err.message}` : ''}`)
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
    this.connect()
    if (opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP socket created${opts.bind ? `, binding to ${opts.host}:${opts.port}` : ''}`)
  }

  connect () {
    if (this.opts.bind) {
      this.server.bind(this.opts.port, this.opts.host)
    }
    this.server.on('message', (msg, rinfo) => {
      if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP message from server: ${msg.length} bytes from ${rinfo.address}:${rinfo.port}`)
      this.event.emit('message', msg, rinfo)
      this.rinfo = rinfo
    })
    this.client.on('message', (response, rinfo) => {
      if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP message from client: ${response.length} bytes from ${rinfo.address}:${rinfo.port}`)
      this.event.emit('message', response)
    })
    this.client.on('error', (err) => {
      console.error(`UDP error: \n${err.stack}`)
      this.client.close()
    })
  }

  write (msg) {
    if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] Writing UDP message: ${msg.length} bytes`)
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
    this._bindListeners()
    this.connect()
    if (opts.log) console.log(`${new Date().toISOString()} [LibNet] Starting UDP piper${opts.client ? ' (client mode)' : ' (server mode)'}`)
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
    if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] Connecting UDP piper`)
    this.removeListeners()
    this.localStream = typeof this.local === 'function' ? this.local() : this.local
    this.remoteStream = typeof this.remote === 'function' ? this.remote() : this.remote
    if (!this.localStream || !this.remoteStream) {
      if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP connect failed (missing streams)`)
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
    if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP listeners attached`)
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
    if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP listeners removed`)
  }

  onLocMessage (msg, rinfo) {
    if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP message from local: ${msg.length} bytes`)
    if (this.remoteStream && !this.destroyed) {
      this.remoteStream.trySend?.(msg)
    }
  }

  onConnectionMessage (msg) {
    if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP message from connection: ${msg.length} bytes`)
    if (this.localStream && !this.destroyed) {
      this.localStream.write?.(msg)
    }
  }

  _handleError (err) {
    if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] UDP error: ${err ? err.message : 'close'}`)
    this.destroy(err)
  }

  destroy (err) {
    if (this.destroyed) return
    this.destroyed = true
    if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] Destroying UDP piper${err ? ` due to error: ${err.message}` : ''}`)
    this.removeListeners()
    try { this.localStream?.destroy?.(err) } catch (e) {}
    try { this.remoteStream?.close?.(err) } catch (e) {}
    if (this.client) {
      if (this.opts.log) console.log(`${new Date().toISOString()} [LibNet] Scheduling retry in ${this.retryDelay}ms`)
      setTimeout(() => {
        this.destroyed = false
        this.connect()
      }, this.retryDelay)
    }
  }
}

function udpConnect (opts, callback) {
  const socketOpts = { ...opts, log: opts.log }
  const socket = new UdpSocket(socketOpts)
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