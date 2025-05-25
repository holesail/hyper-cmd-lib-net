const dgram = require('bare-dgram')
const EventEmitter = require('events')

function connPiper (connection, _dst, opts = {}, stats = {}) {
  const loc = _dst()
  if (loc === null) {
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
    connection.write(d)
  })

  connection.on('data', d => {
    loc.write(d)
  })

  loc.on('error', destroy).on('close', destroy)
  connection.on('error', destroy).on('close', destroy)

  loc.on('end', () => connection.end())
  connection.on('end', () => loc.end())

  loc.on('connect', err => {
    if (opts.debug) {
      console.log('connected')
    }
    if (err) {
      console.error(err)
    }
  })

  function destroy (err) {
    if (destroyed) {
      return
    }

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
  }

  connect () {
    if (this.opts.bind) {
      this.server.bind(this.opts.port, this.opts.host)
    }

    this.server.on('message', (msg, rinfo) => {
      this.event.emit('message', msg, rinfo)
      this.rinfo = rinfo
    })

    this.client.on('message', (response, rinfo) => {
      this.event.emit('message', response)
    })

    this.client.on('error', (err) => {
      console.error(`UDP error: \n${err.stack}`)
      this.client.close()
    })
  }

  write (msg) {
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

    this.removeListeners()

    this.localStream = typeof this.local === 'function' ? this.local() : this.local
    this.remoteStream = typeof this.remote === 'function' ? this.remote() : this.remote

    if (!this.localStream || !this.remoteStream) {
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
  }

  onLocMessage (msg, rinfo) {
    if (this.remoteStream && !this.destroyed) {
      this.remoteStream.trySend?.(msg)
    }
  }

  onConnectionMessage (msg) {
    if (this.localStream && !this.destroyed) {
      this.localStream.write?.(msg)
    }
  }

  _handleError (err) {
    this.destroy(err)
  }

  destroy (err) {
    if (this.destroyed) return
    this.destroyed = true

    this.removeListeners()

    try { this.localStream?.destroy?.(err) } catch (e) {}
    try { this.remoteStream?.close?.(err) } catch (e) {}

    if (this.client) {
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
