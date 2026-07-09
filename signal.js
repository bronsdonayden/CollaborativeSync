// signal.js
// minimal webrtc signaling relay for local dev
// run with: node signal.js

const { WebSocketServer } = require('ws')
const { parse } = require('url')

const PORT = 3000
const wss = new WebSocketServer({ port: PORT })
const rooms = new Map()

wss.on('connection', (socket, req) => {
  const { query } = parse(req.url, true)
  const roomId = query.room

  if (!roomId) { socket.close(1008, 'missing room param'); return }

  if (!rooms.has(roomId)) rooms.set(roomId, [])
  const room = rooms.get(roomId)

  if (room.length >= 2) { socket.close(1008, 'room full'); return }

  room.push(socket)
  console.log(`peer joined room ${roomId} (${room.length}/2)`)

  if (room.length === 2) send(room[0], { type: 'peer-joined' })

  socket.on('message', (raw) => {
    let msg
    try { msg = JSON.parse(raw) } catch { return }
    const peer = room.find(s => s !== socket)
    if (peer) send(peer, { type: msg.type, payload: msg.payload })
  })

  socket.on('close', () => {
    room.splice(room.indexOf(socket), 1)
    console.log(`peer left room ${roomId} (${room.length}/2)`)
    if (room.length === 1) send(room[0], { type: 'peer-disconnected' })
    if (room.length === 0) rooms.delete(roomId)
  })
})

function send(socket, msg) {
  if (socket.readyState === socket.OPEN) socket.send(JSON.stringify(msg))
}

console.log(`signaling server running on ws://localhost:${PORT}`)
