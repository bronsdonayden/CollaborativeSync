// collaborativeSync.js
// webrtc peer to peer crosshair sync for niivue
// initiator volume is ground truth joiner must match it
// data channel stays open on mismatch only sync is gated

;(function () {
  if (window.__boostlet_collab_injected) return
  window.__boostlet_collab_injected = true

  // swap to https://boostlet.org/dist/boostlet.min.js for production
  const BOOSTLET_URL = 'http://localhost:5501/dist/boostlet.min.js'

  // swap to wss://boostlet.org/signal for production
  const SIGNAL_URL = 'ws://localhost:3000'

  const boostletScript = document.createElement('script')
  boostletScript.src = BOOSTLET_URL
  boostletScript.onload = waitForNv
  document.head.appendChild(boostletScript)

  function waitForNv() {
    const poll = setInterval(() => {
      if (!window.nv || window.nv.volumes === undefined) return
      clearInterval(poll)
      Boostlet.init()
      run(window.nv)
    }, 300)
  }

  function run(nv) {
    const params = new URLSearchParams(location.search)
    const isInitiator = !params.get('room')
    const roomId = isInitiator ? generateRoomId() : params.get('room')
    const sharedVolumeUrl = params.get('volume')

    if (isInitiator) {
      const base = nv.volumes && nv.volumes[0]
      const volParam = base && base.url && !base.url.startsWith('blob:')
        ? `&volume=${encodeURIComponent(base.url)}`
        : ''
      history.replaceState(null, '', `?room=${roomId}${volParam}`)
      Boostlet.hint('session started. share this url with your peer', 4000)
    }

    const signal = new WebSocket(`${SIGNAL_URL}?room=${roomId}`)

    let peerConn = null
    let dataChannel = null
    let myVolumeHash = null
    let canonicalHash = null
    let verified = false
    let applyingRemote = false
    let pendingCandidates = []
    let remoteDescSet = false
    let rafPending = false

    onBaseVolumeChanged(nv, async (baseVolume) => {
      myVolumeHash = await hashVolume(baseVolume)
      onVolumeHashUpdated()
    })

    // intercept all writes to nv.opts.sliceType at the assignment level
    // catches every code path regardless of whether setSliceType is called
    let lastSliceType = nv.opts.sliceType
    const sliceDescriptor = Object.getOwnPropertyDescriptor(nv.opts, 'sliceType')
    if (!sliceDescriptor || sliceDescriptor.configurable !== false) {
      Object.defineProperty(nv.opts, 'sliceType', {
        get() { return lastSliceType },
        set(val) {
          lastSliceType = val
          if (applyingRemote) return
          if (!verified || !dataChannel || dataChannel.readyState !== 'open') return
          dataChannel.send(JSON.stringify({ type: 'sliceType', sliceType: val }))
        },
        configurable: true
      })
    } else {
      const prevSliceHandler = nv.onSliceTypeChange
      nv.onSliceTypeChange = function (sliceType) {
        if (prevSliceHandler) prevSliceHandler(sliceType)
        if (applyingRemote) return
        if (!verified || !dataChannel || dataChannel.readyState !== 'open') return
        dataChannel.send(JSON.stringify({ type: 'sliceType', sliceType }))
      }
    }

    signal.onopen = () => {
      signal.send(JSON.stringify({ type: 'join', room: roomId }))
    }

    signal.onerror = () => {
      Boostlet.hint('could not reach signaling server', 4000)
    }

    signal.onmessage = async (raw) => {
      const { type, payload } = JSON.parse(raw.data)

      if (type === 'peer-joined') {
        peerConn = createPeerConnection()
        // unordered and unreliable so fast drags dont queue stale positions behind a dropped packet
        dataChannel = peerConn.createDataChannel('crosshair-sync', { ordered: false, maxRetransmits: 0 })
        wireDataChannel(dataChannel)
        const offer = await peerConn.createOffer()
        await peerConn.setLocalDescription(offer)
        signal.send(JSON.stringify({ type: 'offer', payload: offer }))
      }

      if (type === 'offer') {
        peerConn = createPeerConnection()
        peerConn.ondatachannel = e => wireDataChannel(e.channel)
        await peerConn.setRemoteDescription(payload)
        remoteDescSet = true
        await flushCandidates()
        const answer = await peerConn.createAnswer()
        await peerConn.setLocalDescription(answer)
        signal.send(JSON.stringify({ type: 'answer', payload: answer }))
      }

      if (type === 'answer') {
        await peerConn.setRemoteDescription(payload)
        remoteDescSet = true
        await flushCandidates()
      }

      // buffer candidates that arrive before remote desc is set
      if (type === 'ice') {
        if (remoteDescSet) {
          await peerConn.addIceCandidate(payload)
        } else {
          pendingCandidates.push(payload)
        }
      }
    }

    async function flushCandidates() {
      for (const candidate of pendingCandidates) {
        await peerConn.addIceCandidate(candidate)
      }
      pendingCandidates = []
    }

    function createPeerConnection() {
      const pc = new RTCPeerConnection({
        iceServers: [
          { urls: 'stun:stun.l.google.com:19302' }
          // WARNING add a TURN server before using on institutional or hospital networks
          // stun alone fails silently on symmetric nat which is common on university wifi
        ]
      })
      pc.onicecandidate = e => {
        if (e.candidate) signal.send(JSON.stringify({ type: 'ice', payload: e.candidate }))
      }
      return pc
    }

    function wireDataChannel(channel) {
      dataChannel = channel

      dataChannel.onopen = async () => {
        if (!isInitiator && sharedVolumeUrl && !(nv.volumes && nv.volumes[0])) {
          const ok = await tryLoadSharedVolume(sharedVolumeUrl)
          if (!ok) promptManualUpload()
        }
        if (myVolumeHash) {
          sendHashCheck()
        } else {
          Boostlet.hint('connected. load a volume to begin verification', 3000)
        }
      }

      dataChannel.onmessage = e => {
        const msg = JSON.parse(e.data)
        if (msg.type === 'hash-check') {
          handleHashCheck(msg.hash)
          return
        }
        if (!verified) return
        if (msg.type === 'crosshair') handleCrosshairUpdate(msg)
        if (msg.type === 'sliceType') handleSliceTypeUpdate(msg)
      }
    }

    function onVolumeHashUpdated() {
      if (isInitiator) {
        canonicalHash = myVolumeHash
        verified = true
        Boostlet.hint('volume ready', 2000)
      } else {
        evaluateJoinerState()
      }
      sendHashCheck()
    }

    function handleHashCheck(peerHash) {
      if (isInitiator) {
        const match = myVolumeHash !== null && peerHash === myVolumeHash
        Boostlet.hint(match ? 'peer matched your volume. sync active' : 'waiting for peer to upload correct file', 2000)
        return
      }
      canonicalHash = peerHash
      evaluateJoinerState()
    }

    function evaluateJoinerState() {
      if (canonicalHash === null) {
        verified = false
        Boostlet.hint('waiting for session host to load a volume', 3000)
        return
      }
      if (myVolumeHash === null) {
        verified = false
        Boostlet.hint('please load a volume to join the session', 4000)
        promptManualUpload()
        return
      }
      verified = myVolumeHash === canonicalHash
      if (verified) {
        Boostlet.hint('volume matched. sync active', 2000)
      } else {
        Boostlet.hint('volume mismatch. please upload the same file as the session host', 5000)
        promptManualUpload()
      }
    }

    function sendHashCheck() {
      if (myVolumeHash && dataChannel && dataChannel.readyState === 'open') {
        dataChannel.send(JSON.stringify({ type: 'hash-check', hash: myVolumeHash }))
      }
    }

    async function tryLoadSharedVolume(url) {
      try {
        const res = await fetch(url, { method: 'HEAD', mode: 'cors' })
        if (!res.ok) throw new Error('not reachable')
        await nv.loadVolumes([{ url }])
        return true
      } catch {
        return false
      }
    }

    function promptManualUpload() {
      const input = document.createElement('input')
      input.type = 'file'
      input.accept = '.nii,.nii.gz'
      input.onchange = async (e) => {
        const file = e.target.files[0]
        if (!file) return
        await nv.loadVolumes([{ url: URL.createObjectURL(file), name: file.name }])
      }
      input.click()
    }

    function handleCrosshairUpdate(state) {
      applyingRemote = true
      try {
        nv.scene.crosshairPos = new Float32Array(state.crosshairPos)
        nv.createOnLocationChange()
        nv.drawScene()
      } finally {
        applyingRemote = false
      }
    }

    function handleSliceTypeUpdate(state) {
      if (nv.opts.sliceType === state.sliceType) return
      applyingRemote = true
      try {
        nv.setSliceType(state.sliceType)
      } finally {
        applyingRemote = false
      }
    }

    // chain onto existing handler, never replace it
    const prevHandler = nv.onLocationChange
    nv.onLocationChange = function (loc) {
      if (prevHandler) prevHandler(loc)
      if (applyingRemote) return
      if (!verified || !dataChannel || dataChannel.readyState !== 'open') return
      if (rafPending) return
      rafPending = true
      requestAnimationFrame(() => {
        rafPending = false
        if (dataChannel.readyState !== 'open') return
        dataChannel.send(JSON.stringify({
          type: 'crosshair',
          crosshairPos: Array.from(nv.scene.crosshairPos)
        }))
      })
    }
  }

  // hash volume.img directly not volume.img.buffer
  // if img is a typed array view with nonzero byteOffset then .buffer hashes the wrong bytes
  async function hashVolume(volume) {
    const digest = await crypto.subtle.digest('SHA-256', volume.img)
    return Array.from(new Uint8Array(digest))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('')
  }

  function generateRoomId() {
    const bytes = new Uint8Array(6)
    crypto.getRandomValues(bytes)
    return Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
  }

  function onBaseVolumeChanged(nv, callback) {
    if (nv.volumes && nv.volumes.length > 0) {
      callback(nv.volumes[0])
    }
    if (typeof nv.addEventListener === 'function') {
      nv.addEventListener('imageLoaded', (event) => {
        const loaded = event.detail
        const base = nv.volumes[0]
        if (!base || loaded !== base) return
        callback(base)
      })
      return
    }
    let lastRef = null
    const prev = nv.onImageLoaded
    nv.onImageLoaded = function (volume) {
      if (prev) prev(volume)
      const base = nv.volumes[0]
      if (!base || base === lastRef) return
      lastRef = base
      callback(base)
    }
  }
})()
