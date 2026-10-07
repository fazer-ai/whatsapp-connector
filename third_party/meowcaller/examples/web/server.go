package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"sync"

	meowcaller "github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	qrcode "github.com/skip2/go-qrcode"
)

// videoBridge is an ephemeral, localhost-only HTTP server that pipes a call's H.264 video
// to and from a browser tab via WebCodecs: the browser decodes (display on a canvas) and
// encodes (camera) the H.264; the CLI carries it over the WhatsApp relay through the
// meowcaller Call API. This is a demo/dev tool and lives in the example, not the library —
// the library exposes encoded-frame and participant-attribution APIs while the
// browser owns H.264 pixel encoding and decoding.
//
// The page is self-contained and uses WebCodecs directly, with no JS build step.
type videoBridge struct {
	ln  net.Listener
	srv *http.Server
	log zerolog.Logger

	mu          sync.Mutex
	subs        map[chan vbMsg]struct{}
	onFrame     func([]byte)
	onControl   func(vbControl) error
	orientation int
	qrPNG       []byte
	state       []byte
	groupState  []byte
	groupCallID string
	closed      bool
}

// vbMsg is one SSE payload to the page: a video frame (event "") or an orientation update
// (event "orient").
type vbMsg struct {
	event string
	data  []byte
}

type vbControl struct {
	Action           string   `json:"action"`
	Target           string   `json:"target,omitempty"`
	Targets          []string `json:"targets,omitempty"`
	GroupID          string   `json:"group_id,omitempty"`
	Token            string   `json:"token,omitempty"`
	User             string   `json:"user,omitempty"`
	Emoji            string   `json:"emoji,omitempty"`
	Orientation      int      `json:"orientation,omitempty"`
	Enabled          bool     `json:"enabled,omitempty"`
	ScreenShareID    uint32   `json:"screen_share_id,omitempty"`
	HasScreenShareID bool     `json:"has_screen_share_id,omitempty"`
}

type vbParticipantVideo struct {
	ParticipantID string `json:"participant_id"`
	Sender        string `json:"sender"`
	Device        string `json:"device"`
	PID           uint32 `json:"pid,omitempty"`
	HasPID        bool   `json:"has_pid,omitempty"`
	SSRC          uint32 `json:"ssrc"`
	Orientation   int    `json:"orientation"`
	AccessUnit    string `json:"access_unit"`
}

const browserMutationHeader = "X-Meowcaller-Request"

// newVideoBridge starts a bridge on a free 127.0.0.1 port.
func newVideoBridge(log zerolog.Logger) (*videoBridge, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("video bridge listen: %w", err)
	}
	vb := &videoBridge{ln: ln, log: log, subs: make(map[chan vbMsg]struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/", vb.handleIndex)
	mux.HandleFunc("/in", vb.handleIn)
	mux.HandleFunc("/out", vb.handleOut)
	mux.HandleFunc("/control", vb.handleControl)
	mux.HandleFunc("/qr.png", vb.handleQRCode)
	vb.srv = &http.Server{Handler: mux}
	go func() {
		if err := vb.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			vb.log.Debug().Err(err).Msg("video bridge stopped")
		}
	}()
	return vb, nil
}

// URL is the address to open in a browser.
func (vb *videoBridge) URL() string { return "http://" + vb.ln.Addr().String() }

func (vb *videoBridge) broadcast(m vbMsg) {
	vb.mu.Lock()
	defer vb.mu.Unlock()
	for ch := range vb.subs {
		select {
		case ch <- m:
		default: // page can't keep up — drop (recovers on the next keyframe)
		}
	}
}

// WriteFrame pushes one received Annex-B H.264 access unit to every connected page.
func (vb *videoBridge) WriteFrame(annexB []byte) {
	if len(annexB) == 0 {
		return
	}
	f := make([]byte, len(annexB))
	copy(f, annexB)
	vb.broadcast(vbMsg{data: f})
}

// WriteParticipantFrame pushes one participant-attributed H.264 access unit.
func (vb *videoBridge) WriteParticipantFrame(frame meowcaller.ParticipantVideoFrame) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/group-video-reactions.md#L56-L63
	if len(frame.AccessUnit) == 0 {
		return
	}
	// Source of truth: https://github.com/purpshell/meowcaller/blob/2af70f9b5f88de1ab3b9ba5e9ecda8687810f498/datasheets/group-video-reactions.md#L109-L117
	orientation := frame.Orientation
	if orientation < 0 || orientation > 3 {
		orientation = 0
	}
	data, err := json.Marshal(vbParticipantVideo{
		ParticipantID: frame.ParticipantID,
		Sender:        frame.Sender.String(),
		Device:        frame.Device.String(),
		PID:           frame.PID,
		HasPID:        frame.HasPID,
		SSRC:          frame.SSRC,
		Orientation:   orientation,
		AccessUnit:    base64.StdEncoding.EncodeToString(frame.AccessUnit),
	})
	if err == nil {
		vb.broadcast(vbMsg{event: "participant_video", data: data})
	}
}

// SetOrientation pushes the peer's video device orientation (0..3) so the page can rotate
// the canvas to display upright.
func (vb *videoBridge) SetOrientation(orientation int) {
	vb.mu.Lock()
	if orientation == vb.orientation {
		vb.mu.Unlock()
		return
	}
	vb.orientation = orientation
	vb.mu.Unlock()
	vb.broadcast(vbMsg{event: "orient", data: []byte(strconv.Itoa(orientation))})
}

// OnFrame registers a callback fired per Annex-B access unit the page captures.
func (vb *videoBridge) OnFrame(fn func([]byte)) {
	vb.mu.Lock()
	vb.onFrame = fn
	vb.mu.Unlock()
}

func (vb *videoBridge) OnControl(fn func(vbControl) error) {
	vb.mu.Lock()
	vb.onControl = fn
	vb.mu.Unlock()
}

func (vb *videoBridge) PublishState(state any) {
	data, err := json.Marshal(state)
	if err == nil {
		vb.mu.Lock()
		vb.state = append(vb.state[:0], data...)
		vb.mu.Unlock()
		vb.broadcast(vbMsg{event: "state", data: data})
	}
}

func (vb *videoBridge) PublishEvent(event any) {
	data, err := json.Marshal(event)
	if err == nil {
		vb.broadcast(vbMsg{event: "state", data: data})
	}
}

func (vb *videoBridge) PublishGroupState(state webGroupCallState) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/8a22c339e92fa086d5d2d35569980af734d61c3e/datasheets/web-group-call-outcomes.md#L45-L72
	data, err := json.Marshal(state)
	if err != nil {
		return
	}
	vb.mu.Lock()
	vb.groupState = append(vb.groupState[:0], data...)
	vb.groupCallID = state.CallID
	vb.mu.Unlock()
	vb.broadcast(vbMsg{event: "state", data: data})
}

func (vb *videoBridge) ClearGroupState(callID string) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/8a22c339e92fa086d5d2d35569980af734d61c3e/datasheets/web-group-call-outcomes.md#L45-L72
	vb.mu.Lock()
	if vb.groupCallID == callID {
		vb.groupState = nil
		vb.groupCallID = ""
	}
	vb.mu.Unlock()
}

func (vb *videoBridge) RequestKeyframe() {
	vb.broadcast(vbMsg{event: "keyframe", data: []byte("1")})
}

func (vb *videoBridge) SetQRCode(code string) error {
	png, err := qrcode.Encode(code, qrcode.Medium, 320)
	if err != nil {
		return err
	}
	vb.setQRCodePNG(png)
	return nil
}

func (vb *videoBridge) setQRCodePNG(png []byte) {
	vb.mu.Lock()
	vb.qrPNG = append(vb.qrPNG[:0], png...)
	vb.mu.Unlock()
}

// Close stops the server and releases page subscriptions.
func (vb *videoBridge) Close() error {
	vb.mu.Lock()
	if vb.closed {
		vb.mu.Unlock()
		return nil
	}
	vb.closed = true
	for ch := range vb.subs {
		close(ch)
		delete(vb.subs, ch)
	}
	vb.mu.Unlock()
	return vb.srv.Close()
}

func (vb *videoBridge) handleIn(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "no flush", http.StatusInternalServerError)
		return
	}
	ch := make(chan vbMsg, 16)
	vb.mu.Lock()
	if vb.closed {
		vb.mu.Unlock()
		return
	}
	vb.subs[ch] = struct{}{}
	orient := vb.orientation
	state := append([]byte(nil), vb.state...)
	groupState := append([]byte(nil), vb.groupState...)
	vb.mu.Unlock()
	defer func() {
		vb.mu.Lock()
		if _, ok := vb.subs[ch]; ok {
			delete(vb.subs, ch)
			close(ch)
		}
		vb.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	fmt.Fprintf(w, "event: orient\ndata: %d\n\n", orient) // send current orientation up front
	if len(state) > 0 {
		fmt.Fprintf(w, "event: state\ndata: %s\n\n", state)
	}
	if len(groupState) > 0 {
		fmt.Fprintf(w, "event: state\ndata: %s\n\n", groupState)
	}
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			if m.event != "" {
				fmt.Fprintf(w, "event: %s\ndata: %s\n\n", m.event, m.data)
			} else {
				fmt.Fprintf(w, "data: %s\n\n", base64.StdEncoding.EncodeToString(m.data))
			}
			flusher.Flush()
		}
	}
}

func (vb *videoBridge) handleOut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !allowBrowserMutation(w, r, "application/octet-stream") {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		http.Error(w, "read", http.StatusBadRequest)
		return
	}
	vb.mu.Lock()
	fn := vb.onFrame
	vb.mu.Unlock()
	if fn != nil && len(body) > 0 {
		fn(body)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (vb *videoBridge) handleControl(w http.ResponseWriter, r *http.Request) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/f62ccfb2a431fc25008423954287fd3009fed161/datasheets/web-initial-group-call.md#L40-L120
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !allowBrowserMutation(w, r, "application/json") {
		return
	}
	var command vbControl
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	valid := map[string]bool{
		"dial_audio": true, "dial_video": true, "answer": true, "reject": true,
		"start_group_audio": true, "start_group_video": true,
		"start_group_id_audio": true, "start_group_id_video": true,
		"start_video": true, "accept_video": true, "stop_video": true,
		"enable_video": true, "disable_video": true,
		"hangup": true, "orientation": true, "reaction": true,
		"create_call_link_audio": true, "create_call_link_video": true,
		"preview_call_link_audio": true, "preview_call_link_video": true,
		"join_call_link_audio": true, "join_call_link_video": true,
		"set_approval_required": true,
		"admit_waiting_user":    true, "deny_waiting_user": true,
		"raise_hand": true, "lower_hand": true,
		"start_screen_share": true, "stop_screen_share": true,
		// Source of truth: https://github.com/purpshell/meowcaller/blob/302ff288df89adef44cda74f74da6285b6f13aa2/datasheets/web-group-participant-invite.md#L23-L94
		"add_participants": true, "ring_participant": true,
	}
	if !valid[command.Action] {
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	vb.mu.Lock()
	fn := vb.onControl
	vb.mu.Unlock()
	if fn == nil {
		http.Error(w, "call controller unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := fn(command); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func allowBrowserMutation(w http.ResponseWriter, r *http.Request, expectedMediaType string) bool {
	// Source of truth: https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS#preflighted_requests
	if r.Header.Get(browserMutationHeader) != "1" {
		http.Error(w, "missing browser request header", http.StatusForbidden)
		return false
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != expectedMediaType {
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return false
	}
	return true
}

func (vb *videoBridge) handleQRCode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	vb.mu.Lock()
	png := append([]byte(nil), vb.qrPNG...)
	vb.mu.Unlock()
	if len(png) == 0 {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(png)
}

func (vb *videoBridge) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, videoBridgePage)
}

const videoBridgePage = `<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>meowcaller call console</title>
<style>
:root{color-scheme:dark;--bg:#111315;--panel:#1b1f22;--line:#353b40;--text:#f1f3f4;--muted:#a7afb5;--green:#39b87f;--red:#e25d5d;--amber:#d5a542}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px system-ui,sans-serif;letter-spacing:0}
header{height:56px;padding:0 20px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:16px}h1{font-size:16px;margin:0}.state{color:var(--muted);margin-left:auto}
main{max-width:1280px;margin:auto;padding:16px}.toolbar,.invite-toolbar{display:grid;grid-template-columns:minmax(180px,1fr) repeat(2,auto);gap:8px;margin-bottom:10px}
input,textarea,button{min-height:38px;border:1px solid var(--line);border-radius:6px;background:#202529;color:var(--text);font:inherit;letter-spacing:0}
input,textarea{padding:9px 11px;min-width:0}textarea{resize:vertical}button{padding:0 13px;cursor:pointer;white-space:nowrap}button:hover{border-color:#68727a}button:disabled{cursor:not-allowed;opacity:.45}button.primary{background:#176846;border-color:#25865e}button.danger{background:#7b2929;border-color:#a83b3b}
.actions{display:flex;gap:8px;overflow-x:auto;padding-bottom:10px}.reaction-picker{display:flex;gap:6px;padding-bottom:12px}.reaction-picker button{width:38px;padding:0;font-size:20px}.media{display:grid;grid-template-columns:1fr 1fr;gap:12px;border-top:1px solid var(--line);padding-top:14px}
.pane{min-width:0}.pane-head{height:42px;color:var(--muted);font-size:12px;text-transform:uppercase;display:flex;align-items:center;justify-content:space-between}.pane-head button{height:32px}.remote-wrap,.local-wrap,.participant-video{position:relative;display:grid;place-items:center;width:100%;aspect-ratio:4/3;overflow:hidden;background:#050607;border:1px solid var(--line);border-radius:6px}.group-videos{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px;width:100%}.group-videos[hidden],.remote-wrap[hidden]{display:none}.participant-label{position:absolute;left:8px;bottom:6px;z-index:2;max-width:calc(100% - 16px);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:3px 6px;border-radius:4px;background:#000a;color:#fff;font-size:11px}.reactions{position:absolute;inset:0;overflow:hidden;pointer-events:none;display:flex;align-items:center;justify-content:center;z-index:3}.reaction{position:absolute;font-size:64px;animation:reaction-rise 1.8s ease-out forwards}@keyframes reaction-rise{0%{opacity:0;transform:translateY(24px) scale(.7)}20%{opacity:1;transform:translateY(0) scale(1)}75%{opacity:1}100%{opacity:0;transform:translateY(-80px) scale(1.15)}}
canvas,video{display:block;width:auto;height:auto;max-width:100%;max-height:100%;object-fit:contain;background:#050607}
#log{height:150px;overflow:auto;margin-top:12px;padding:10px;border:1px solid var(--line);background:#0b0d0e;color:#b8d8c8;font:12px ui-monospace,monospace;white-space:pre-wrap}
.invite-note{color:var(--muted);font-size:12px;margin:-3px 0 10px}
.waiting-room{display:grid;gap:6px;margin:0 0 12px}.waiting-room-empty{color:var(--muted);font-size:12px}.waiting-user{display:grid;grid-template-columns:minmax(0,1fr) auto auto;align-items:center;gap:8px;padding:7px 9px;border:1px solid var(--line);border-radius:6px;background:var(--panel)}.waiting-user-state{color:var(--muted);font-size:12px}.waiting-user-actions{display:flex;gap:6px}.waiting-user-actions button{min-height:30px}
.participant-roster{display:grid;gap:6px;margin:0 0 12px}.participant-row{display:grid;grid-template-columns:minmax(0,1fr) auto auto;align-items:center;gap:8px;padding:7px 9px;border:1px solid var(--line);border-radius:6px;background:var(--panel)}.participant-identity{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.participant-state{color:var(--muted);font-size:12px}.participant-row button{min-height:30px}
.pairing{display:flex;align-items:center;gap:18px;padding:12px 0 16px;border-bottom:1px solid var(--line);margin-bottom:14px}.pairing[hidden]{display:none}.pairing img{width:180px;height:180px;background:#fff;border-radius:6px}.pairing strong{display:block;margin-bottom:5px}.pairing span{color:var(--muted)}
@media(max-width:760px){header{padding:0 12px}.toolbar,.invite-toolbar{grid-template-columns:1fr 1fr}.toolbar input,.invite-toolbar textarea{grid-column:1/-1}.media{grid-template-columns:1fr}.actions{flex-wrap:wrap}.actions button,.toolbar button,.invite-toolbar button{flex:1 0 auto}}
</style></head><body>
<header><h1>meowcaller call console</h1><div id="state" class="state">idle</div></header>
<main>
  <section id="pairing" class="pairing" hidden><img id="qr" alt="WhatsApp linked-device QR"><div><strong>Link WhatsApp</strong><span>WhatsApp > Linked devices > Link a device</span></div></section>
  <div class="toolbar"><input id="target" inputmode="tel" placeholder="WhatsApp number or LID"><button id="dialAudio">Dial audio</button><button id="dialVideo" class="primary">Dial video</button></div>
  <div class="actions"><button id="createCallLinkAudio">Create audio link</button><button id="createCallLinkVideo" class="primary">Create video link</button></div>
  <div class="toolbar"><input id="generatedCallLink" aria-label="Generated call link" placeholder="Your generated call link appears here" readonly><button id="copyGeneratedCallLink" disabled>Copy link</button><button id="joinGeneratedCallLink" disabled>Join created link</button></div>
  <div class="toolbar"><input id="callLink" placeholder="Paste an existing call-link token or URL"><button id="previewCallLinkAudio">Preview audio link</button><button id="previewCallLinkVideo">Preview video link</button></div>
  <div class="actions"><button id="joinCallLinkAudio">Join audio link</button><button id="joinCallLinkVideo" class="primary">Join video link</button><label><input id="approvalRequired" type="checkbox"> Require approval</label></div>
  <div id="waitingRoom" class="waiting-room" aria-live="polite"><div class="waiting-room-empty">Waiting room: no authoritative state yet.</div></div>
  <div class="toolbar"><input id="groupID" placeholder="WhatsApp group ID or @g.us JID"><button id="startGroupIDAudio">Call group audio</button><button id="startGroupIDVideo" class="primary">Call group video</button></div>
  <div class="invite-toolbar"><textarea id="participants" rows="2" placeholder="People (comma or newline separated)"></textarea><button id="startGroupAudio" disabled>Start group audio</button><button id="startGroupVideo" disabled>Start group video</button><button id="addParticipants" disabled>Add people</button></div>
  <div id="participantStatus" class="invite-note">Invites are submitted; waiting for roster confirmation.</div>
  <div id="participantRoster" class="participant-roster" aria-live="polite"></div>
  <div class="actions"><button id="answer">Answer</button><button id="reject">Reject</button><button id="startVideo">Upgrade to video</button><button id="acceptVideo" hidden>Accept video</button><button id="stopVideo">Stop video</button><button id="raiseHand">Raise hand</button><button id="lowerHand">Lower hand</button><button id="shareScreen">Share screen</button><button id="hangup" class="danger">Hang up</button></div>
  <div class="reaction-picker"><button data-reaction="👍" aria-label="Thumbs up">👍</button><button data-reaction="❤️" aria-label="Heart">❤️</button><button data-reaction="😂" aria-label="Laugh">😂</button><button data-reaction="😮" aria-label="Surprised">😮</button><button data-reaction="😢" aria-label="Sad">😢</button><button data-reaction="🙏" aria-label="Thanks">🙏</button><button data-reaction="" aria-label="Remove reaction">×</button><input id="customReaction" aria-label="Custom emoji reaction" placeholder="Any emoji"><button id="sendCustomReaction">Send</button></div>
  <div class="media">
    <section class="pane"><div class="pane-head"><span>WhatsApp participants</span><span id="remoteMeta">waiting</span></div><div id="remoteWrap" class="remote-wrap"><canvas id="remote" width="640" height="480"></canvas><div id="reactions" class="reactions" aria-live="polite"></div></div><div id="groupVideos" class="group-videos" hidden></div></section>
    <section class="pane"><div class="pane-head"><span>Local camera</span><button id="cam">Start camera</button></div><div class="local-wrap"><video id="local" autoplay muted playsinline></video></div></section>
  </div>
  <div id="log"></div>
</main>
<script>
const $=id=>document.getElementById(id), log=(...a)=>{$('log').textContent+=a.join(' ')+'\n';$('log').scrollTop=$('log').scrollHeight};
const mutationHeaders={'X-Meowcaller-Request':'1'},control=async(action,extra={})=>{const r=await fetch('/control',{method:'POST',headers:{...mutationHeaders,'content-type':'application/json'},body:JSON.stringify({action,...extra})});if(!r.ok)throw Error((await r.text()).trim())};
const invoke=(action,extra)=>control(action,extra).catch(e=>log(action,e.message));
let generatedCallLinkVideo=false;
async function createCallLink(action){$('createCallLinkAudio').disabled=true;$('createCallLinkVideo').disabled=true;$('generatedCallLink').value='';$('copyGeneratedCallLink').disabled=true;$('joinGeneratedCallLink').disabled=true;try{await control(action)}catch(e){log(action,e.message)}finally{$('createCallLinkAudio').disabled=false;$('createCallLinkVideo').disabled=false}}
function updatePeopleControls(s){if(s.event==='idle'||s.event==='ended'){$('startGroupAudio').disabled=false;$('startGroupVideo').disabled=false;$('startGroupIDAudio').disabled=false;$('startGroupIDVideo').disabled=false;$('addParticipants').disabled=true}else if(s.event==='ready'||(s.event==='phase'&&s.phase===4)){$('startGroupAudio').disabled=true;$('startGroupVideo').disabled=true;$('startGroupIDAudio').disabled=true;$('startGroupIDVideo').disabled=true;$('addParticipants').disabled=false}else if(['incoming','dialing','group_dialing','answering'].includes(s.event)||s.event==='phase'||s.event==='pairing'){$('startGroupAudio').disabled=true;$('startGroupVideo').disabled=true;$('startGroupIDAudio').disabled=true;$('startGroupIDVideo').disabled=true;$('addParticipants').disabled=true}}
$('dialAudio').onclick=()=>invoke('dial_audio',{target:$('target').value.trim()});
$('createCallLinkAudio').onclick=()=>createCallLink('create_call_link_audio');$('createCallLinkVideo').onclick=()=>createCallLink('create_call_link_video');
$('copyGeneratedCallLink').onclick=()=>navigator.clipboard.writeText($('generatedCallLink').value).then(()=>log('call link copied')).catch(e=>log('copy call link',e.message));
$('joinGeneratedCallLink').onclick=()=>{const extra={token:$('generatedCallLink').value};if(generatedCallLinkVideo)invokeVideoCall('join_call_link_video',extra);else invoke('join_call_link_audio',extra)};
$('previewCallLinkAudio').onclick=()=>invoke('preview_call_link_audio',{token:$('callLink').value.trim()});$('previewCallLinkVideo').onclick=()=>invoke('preview_call_link_video',{token:$('callLink').value.trim()});
$('joinCallLinkAudio').onclick=()=>invoke('join_call_link_audio',{token:$('callLink').value.trim()});$('joinCallLinkVideo').onclick=()=>invokeVideoCall('join_call_link_video',{token:$('callLink').value.trim()});
$('approvalRequired').onchange=()=>invoke('set_approval_required',{enabled:$('approvalRequired').checked});
$('startGroupIDAudio').onclick=()=>invoke('start_group_id_audio',{group_id:$('groupID').value.trim()});
$('startGroupAudio').onclick=()=>invoke('start_group_audio',{targets:participantTargets()});$('addParticipants').onclick=()=>invoke('add_participants',{targets:participantTargets()});const participantTargets=()=>$('participants').value.split(/[,\n]/).map(target=>target.trim()).filter(Boolean);
$('answer').onclick=()=>invoke('answer');$('reject').onclick=()=>invoke('reject');$('acceptVideo').onclick=()=>invoke('accept_video');$('hangup').onclick=()=>invoke('hangup');
document.querySelectorAll('[data-reaction]').forEach(b=>b.onclick=()=>invoke('reaction',{emoji:b.dataset.reaction}));
$('sendCustomReaction').onclick=()=>invoke('reaction',{emoji:$('customReaction').value});
$('raiseHand').onclick=()=>invoke('raise_hand');$('lowerHand').onclick=()=>invoke('lower_hand');
if(!('VideoDecoder'in window))log('WebCodecs unavailable in this browser');
const remote=$('remote'),paint=remote.getContext('2d'),es=new EventSource('/in');let decoder=null,decodeStarted=false,forceKeyframe=true,remoteVideoActive=true,remoteOrientation=0,groupMode=false;const participantRenderers=new Map();
function keyNAL(d){for(let i=0;i+4<d.length;i++){let p=-1;if(d[i]===0&&d[i+1]===0&&d[i+2]===1)p=i+3;else if(d[i]===0&&d[i+1]===0&&d[i+2]===0&&d[i+3]===1)p=i+4;if(p>=0){const t=d[p]&31;if(t===5||t===7)return true}}return false}
function setRemoteVideoActive(active){remoteVideoActive=active;if(active){$('remoteMeta').textContent='waiting'}else{paint.clearRect(0,0,remote.width,remote.height);$('remoteMeta').textContent='off'}}
function drawRemoteFrame(f){const w=f.displayWidth,h=f.displayHeight,q=((remoteOrientation%4)+4)%4,portrait=q%2===1;remote.width=portrait?h:w;remote.height=portrait?w:h;paint.save();if(q===1){paint.translate(remote.width,0);paint.rotate(Math.PI/2)}else if(q===2){paint.translate(remote.width,remote.height);paint.rotate(Math.PI)}else if(q===3){paint.translate(0,remote.height);paint.rotate(-Math.PI/2)}paint.drawImage(f,0,0,w,h);paint.restore();$('remoteMeta').textContent=remote.width+'x'+remote.height}
function resetRemoteDecoder(){decodeStarted=false;if(decoder&&decoder.state!=='closed')decoder.close();decoder=null}
function getDecoder(){if(decoder&&decoder.state!=='closed')return decoder;decoder=new VideoDecoder({output:f=>{if(remoteVideoActive)drawRemoteFrame(f);f.close()},error:e=>{resetRemoteDecoder();log('decoder',e.message)}});decoder.configure({codec:'avc1.42E01F',optimizeForLatency:true});return decoder}
function participantKey(v){return[v.participant_id||v.sender||'participant',v.device||'',v.has_pid?'pid'+v.pid:'',String(v.ssrc)].filter(Boolean).join('|')}
const raisedHands=new Map(),screenSharers=new Set();let rosterParticipants=[];
function resetParticipantDecoder(r){r.started=false;if(r.decoder&&r.decoder.state!=='closed')r.decoder.close();r.decoder=null}
function getParticipantDecoder(r){if(r.decoder&&r.decoder.state!=='closed')return r.decoder;r.decoder=new VideoDecoder({output:f=>{drawParticipantFrame(r,f);f.close()},error:e=>{resetParticipantDecoder(r);log('participant decoder',r.key,e.message)}});r.decoder.configure({codec:'avc1.42E01F',optimizeForLatency:true});return r.decoder}
function resetGroupMode(){for(const r of participantRenderers.values())resetParticipantDecoder(r);participantRenderers.clear();raisedHands.clear();screenSharers.clear();rosterParticipants=[];$('participantRoster').replaceChildren();$('groupVideos').replaceChildren();$('groupVideos').hidden=true;$('remoteWrap').hidden=false;groupMode=false}
function drawParticipantFrame(r,f){const w=f.displayWidth,h=f.displayHeight,q=((r.orientation%4)+4)%4,portrait=q%2===1;r.canvas.width=portrait?h:w;r.canvas.height=portrait?w:h;r.paint.save();if(q===1){r.paint.translate(r.canvas.width,0);r.paint.rotate(Math.PI/2)}else if(q===2){r.paint.translate(r.canvas.width,r.canvas.height);r.paint.rotate(Math.PI)}else if(q===3){r.paint.translate(0,r.canvas.height);r.paint.rotate(-Math.PI/2)}r.paint.drawImage(f,0,0,w,h);r.paint.restore()}
function refreshParticipantLabel(r){const hand=raisedHands.get(r.sender)||raisedHands.get(r.device)||raisedHands.get(r.key)||false,sharing=screenSharers.has(r.sender)||screenSharers.has(r.device)||screenSharers.has(r.key);r.label.textContent=(hand?'Raised hand · ':'')+(sharing?'Screen share · ':'')+r.baseLabel}
function ensureParticipantRenderer(v){const key=participantKey(v);let r=participantRenderers.get(key);if(r){r.sender=v.sender||r.sender;r.device=v.device||r.device;refreshParticipantLabel(r);return r}const wrap=document.createElement('div');wrap.className='participant-video';const canvas=document.createElement('canvas');canvas.width=640;canvas.height=480;const label=document.createElement('span');label.className='participant-label';const baseLabel=v.sender||v.device||key;const reactions=document.createElement('div');reactions.className='reactions';wrap.append(canvas,reactions,label);$('groupVideos').appendChild(wrap);r={key,sender:v.sender,device:v.device,baseLabel,orientation:v.orientation||0,wrap,canvas,paint:canvas.getContext('2d'),reactions,label,started:false,decoder:null};refreshParticipantLabel(r);getParticipantDecoder(r);participantRenderers.set(key,r);return r}
function showReaction(emoji,sender){if(!emoji)return;let host=$('reactions');if(groupMode&&sender){for(const r of participantRenderers.values()){if(r.sender===sender||r.device===sender||r.key===sender){host=r.reactions;break}}}const el=document.createElement('span');el.className='reaction';el.textContent=emoji;host.appendChild(el);setTimeout(()=>el.remove(),1800)}
function renderWaitingRoom(s){$('approvalRequired').checked=!!s.enabled;const host=$('waitingRoom');host.replaceChildren();if(s.in_waiting_room){const pending=document.createElement('div');pending.className='waiting-room-empty';pending.textContent='Waiting for administrator approval.';host.appendChild(pending);return}const users=s.users||[];if(!users.length){const empty=document.createElement('div');empty.className='waiting-room-empty';empty.textContent=s.enabled?'Waiting room is empty.':'Approval is disabled.';host.appendChild(empty);return}for(const u of users){const row=document.createElement('div');row.className='waiting-user';const identity=document.createElement('span');identity.className='participant-identity';identity.textContent=u.pn||u.jid||'user';identity.title=u.jid||'';const state=document.createElement('span');state.className='waiting-user-state';state.textContent=u.state;row.append(identity,state);if(s.is_admin&&u.state==='waiting_room_joined'){const actions=document.createElement('span');actions.className='waiting-user-actions';const approve=document.createElement('button');approve.className='primary';approve.textContent='Approve';approve.onclick=()=>invoke('admit_waiting_user',{user:u.jid});const reject=document.createElement('button');reject.className='danger';reject.textContent='Reject';reject.onclick=()=>invoke('deny_waiting_user',{user:u.jid});actions.append(approve,reject);row.appendChild(actions)}host.appendChild(row)}}
function markRaisedHand(participant,raised){raisedHands.set(participant,!!raised);for(const r of participantRenderers.values())if(r.sender===participant||r.device===participant||r.key===participant)refreshParticipantLabel(r);renderParticipantRoster()}
function markScreenShare(participant,active){if(active)screenSharers.add(participant);else screenSharers.delete(participant);for(const r of participantRenderers.values())if(r.sender===participant||r.device===participant||r.key===participant){resetParticipantDecoder(r);refreshParticipantLabel(r)}renderParticipantRoster()}
function renderParticipantRoster(participants){if(participants)rosterParticipants=participants.slice();const host=$('participantRoster');host.replaceChildren();for(const p of rosterParticipants){if(!raisedHands.has(p.jid))raisedHands.set(p.jid,!!p.hand_raised);const hand=raisedHands.get(p.jid),sharing=screenSharers.has(p.jid);const row=document.createElement('div');row.className='participant-row';const identity=document.createElement('span');identity.className='participant-identity';identity.textContent=p.pn||p.jid;identity.title=p.jid;const state=document.createElement('span');state.className='participant-state';const endpoints=(p.devices||[]).filter(d=>d.has_pid).length;state.textContent=p.state+(hand?' · raised hand':'')+(sharing?' · sharing screen':'')+(endpoints?' · '+endpoints+' endpoint'+(endpoints===1?'':'s'):'');row.append(identity,state);if(p.can_ring){const ring=document.createElement('button');ring.textContent='Ring';ring.onclick=()=>invoke('ring_participant',{target:p.jid});row.appendChild(ring)}host.appendChild(row)}}
function reconcileParticipantRenderers(participants){const activeParticipants=new Set(),activeDevices=new Set();for(const p of participants||[]){if(p.state!=='connected')continue;activeParticipants.add(p.jid);for(const d of p.devices||[])if(d.has_pid)activeDevices.add(d.jid)}for(const[key,r]of participantRenderers){const keep=(r.device&&activeDevices.has(r.device))||(!r.device&&activeParticipants.has(r.sender));if(!keep){resetParticipantDecoder(r);r.wrap.remove();participantRenderers.delete(key)}}}
es.onmessage=e=>{const au=Uint8Array.from(atob(e.data),c=>c.charCodeAt(0)),key=keyNAL(au);if(!decodeStarted&&!key)return;decodeStarted=true;try{getDecoder().decode(new EncodedVideoChunk({type:key?'key':'delta',timestamp:performance.now()*1000,data:au}))}catch(err){log('decode',err.message);decodeStarted=false}};
es.addEventListener('participant_video',e=>{const v=JSON.parse(e.data),au=Uint8Array.from(atob(v.access_unit),c=>c.charCodeAt(0)),key=keyNAL(au);if(!groupMode){remoteOrientation=v.orientation||0;if(!decodeStarted&&!key)return;decodeStarted=true;try{getDecoder().decode(new EncodedVideoChunk({type:key?'key':'delta',timestamp:performance.now()*1000,data:au}))}catch(err){resetRemoteDecoder();log('decode',err.message)}return}const r=ensureParticipantRenderer(v);r.orientation=v.orientation||0;if(!r.started&&!key)return;r.started=true;try{getParticipantDecoder(r).decode(new EncodedVideoChunk({type:key?'key':'delta',timestamp:performance.now()*1000,data:au}))}catch(err){resetParticipantDecoder(r);log('participant decode',err.message)}});
es.addEventListener('orient',e=>{remoteOrientation=+e.data||0});es.addEventListener('keyframe',()=>{forceKeyframe=true;log('peer requested keyframe')});
es.addEventListener('state',e=>{const s=JSON.parse(e.data);updatePeopleControls(s);if(s.event!=='reaction'&&s.event!=='participant_invite'&&s.event!=='participant_join'&&s.event!=='group_state'&&s.event!=='hand_raise'&&s.event!=='screen_share')$('state').textContent=s.event+(s.peer?' / '+s.peer:'');if(s.event==='pairing'){$('pairing').hidden=false;$('qr').src='/qr.png?t='+Date.now()}else if(s.event==='idle'){$('pairing').hidden=true;resetGroupMode()}else if(s.event==='ended')resetGroupMode();if(s.event==='call_link_created'){generatedCallLinkVideo=!!s.video;$('generatedCallLink').value=s.url||s.token;$('copyGeneratedCallLink').disabled=false;$('joinGeneratedCallLink').disabled=false;$('joinGeneratedCallLink').textContent=s.video?'Join created video link':'Join created audio link'}else if(s.event==='waiting_room')renderWaitingRoom(s);else if(s.event==='hand_raise')markRaisedHand(s.participant,s.raised);else if(s.event==='screen_share'){markScreenShare(s.participant,s.active);if(!groupMode)resetRemoteDecoder();$('remoteMeta').textContent=s.active?'screen share':'camera'}else if(s.event==='video_state'){if(s.video_state===0||s.video_state===6)setRemoteVideoActive(false);else if(s.video_state===1)setRemoteVideoActive(true);$('acceptVideo').hidden=!(s.video_state===3||s.video_state===11)}else if(s.event==='reaction'&&!s.removed)showReaction(s.emoji,s.sender);else if(s.event==='group_state'){groupMode=true;$('remoteWrap').hidden=true;$('groupVideos').hidden=false;const participants=s.participants||[];renderParticipantRoster(participants);reconcileParticipantRenderers(participants);const connected=participants.reduce((n,p)=>n+(p.state==='connected'?(p.devices||[]).filter(d=>d.has_pid).length:0),0);$('participantStatus').textContent='Roster tx '+s.transaction_id+': '+connected+' connected endpoint'+(connected===1?'':'s');for(const p of participants)markRaisedHand(p.jid,!!p.hand_raised)}else if(s.event==='participant_join'){$('participantStatus').textContent=s.target+' joined as '+s.participant+' (PID '+s.pid+')'}const safeState={...s};if(safeState.event==='call_link_created'){delete safeState.url;delete safeState.token}log(new Date().toLocaleTimeString(),JSON.stringify(safeState))});es.onerror=()=>log('event stream disconnected');
let stream=null,encoder=null,reader=null,upload=Promise.resolve();
const avcLevel31MaxWidth=1280,avcLevel31MaxHeight=720;
function avcLevel31Size(width,height){const scale=Math.min(1,avcLevel31MaxWidth/width,avcLevel31MaxHeight/height);return{width:Math.max(2,Math.floor(width*scale/2)*2),height:Math.max(2,Math.floor(height*scale/2)*2)}}
async function stopCamera(){if(reader)await reader.cancel().catch(()=>{});if(encoder&&encoder.state!=='closed')encoder.close();if(stream)stream.getTracks().forEach(t=>t.stop());stream=encoder=reader=null;$('local').srcObject=null;$('cam').textContent='Start camera'}
async function pumpCamera(activeReader,activeEncoder){let n=0,encodedWidth=0,encodedHeight=0;try{for(;;){const{value:f,done}=await activeReader.read();if(done)break;const size=avcLevel31Size(f.displayWidth,f.displayHeight);if(size.width!==encodedWidth||size.height!==encodedHeight){encodedWidth=size.width;encodedHeight=size.height;activeEncoder.configure({codec:'avc1.42E01F',avc:{format:'annexb'},width:size.width,height:size.height,framerate:15,bitrate:Math.max(250000,Math.min(2000000,size.width*size.height*2)),latencyMode:'realtime'});forceKeyframe=true}if(activeEncoder.state==='configured'&&activeEncoder.encodeQueueSize<2){const key=forceKeyframe||n%15===0;forceKeyframe=false;activeEncoder.encode(f,{keyFrame:key});n++}f.close()}}catch(e){if(reader===activeReader){log('camera',e.message);await stopCamera()}}}
async function startCapture(nextStream,label){stream=nextStream;$('local').srcObject=stream;$('cam').textContent=label;const track=stream.getVideoTracks()[0];encoder=new VideoEncoder({output:chunk=>{const b=new Uint8Array(chunk.byteLength);chunk.copyTo(b);upload=upload.then(()=>fetch('/out',{method:'POST',headers:{...mutationHeaders,'content-type':'application/octet-stream'},body:b})).then(r=>{if(!r.ok)throw Error('video upload '+r.status)}).catch(e=>log(e.message))},error:e=>log('encoder',e.message)});reader=new MediaStreamTrackProcessor({track}).readable.getReader();void pumpCamera(reader,encoder);return true}
async function startCamera(){if(stream)return true;try{return await startCapture(await navigator.mediaDevices.getUserMedia({video:{width:{ideal:1280,max:1280},height:{ideal:720,max:720},frameRate:{ideal:15,max:15}}}),'Stop camera')}catch(e){log('camera',e.message);await stopCamera();return false}}
let sharingScreen=false,cameraWasActive=false;
async function stopScreenShare(){if(!sharingScreen)return;try{await control('stop_screen_share')}catch(e){log('stop_screen_share',e.message);return}sharingScreen=false;await stopCamera();if(cameraWasActive)await startCamera();cameraWasActive=false;$('shareScreen').textContent='Share screen'}
async function startScreenShare(){if(sharingScreen){await stopScreenShare();return}cameraWasActive=!!stream;let display=null,screenShareStarted=false;try{display=await navigator.mediaDevices.getDisplayMedia({video:true,audio:false});if(stream)await stopCamera();await control('start_screen_share',{screen_share_id:1,has_screen_share_id:true});screenShareStarted=true;sharingScreen=true;forceKeyframe=true;await startCapture(display,'Sharing screen');display.getVideoTracks()[0].onended=()=>void stopScreenShare();$('shareScreen').textContent='Stop sharing'}catch(e){log('start_screen_share',e.message);if(screenShareStarted)await control('stop_screen_share').catch(stopError=>log('stop_screen_share',stopError.message));sharingScreen=false;if(display)display.getTracks().forEach(t=>t.stop());await stopCamera();if(cameraWasActive)await startCamera();cameraWasActive=false}}
$('shareScreen').onclick=()=>void startScreenShare();
async function invokeVideoCall(action,extra){const startedHere=!stream;if(!await startCamera())return;try{await control(action,extra)}catch(e){log(action,e.message);if(startedHere)await stopCamera()}}
$('dialVideo').onclick=()=>invokeVideoCall('dial_video',{target:$('target').value.trim()});
$('startGroupVideo').onclick=()=>invokeVideoCall('start_group_video',{targets:participantTargets()});
$('startGroupIDVideo').onclick=()=>invokeVideoCall('start_group_id_video',{group_id:$('groupID').value.trim()});
$('startVideo').onclick=()=>invokeVideoCall('start_video');
$('stopVideo').onclick=async()=>{try{await control('stop_video');await stopCamera()}catch(e){log('stop_video',e.message)}};
$('cam').onclick=async()=>{if(stream){await stopCamera();invoke('disable_video');return}if(await startCamera())invoke('enable_video')};
</script></body></html>`
