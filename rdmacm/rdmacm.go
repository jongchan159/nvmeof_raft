// +build raft

package rdmacm

/*
#cgo LDFLAGS: -lrdmacm -libverbs
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <stdint.h>

static int get_errno(void) { return errno; }

#define RDMA_RECV_DEPTH   64
#define RDMA_FRAG_SIZE    65536
#define RDMA_HDR_SIZE     8
#define RDMA_PAYLOAD_MAX  (RDMA_FRAG_SIZE - RDMA_HDR_SIZE)

typedef struct {
    struct rdma_event_channel *ec;
    struct rdma_cm_id         *id;
    struct ibv_pd             *pd;
    struct ibv_comp_channel   *comp_ch;
    struct ibv_cq             *send_cq;
    struct ibv_cq             *recv_cq;
    struct ibv_qp             *qp;
    uint8_t      *send_buf;
    struct ibv_mr *send_mr;
    uint8_t      *recv_buf;
    struct ibv_mr *recv_mr;
} rdma_ctx_t;

static int post_recv_slot(rdma_ctx_t *c, uint32_t slot) {
    struct ibv_sge sge = {
        .addr   = (uint64_t)(c->recv_buf + (size_t)slot * RDMA_FRAG_SIZE),
        .length = RDMA_FRAG_SIZE,
        .lkey   = c->recv_mr->lkey,
    };
    struct ibv_recv_wr wr = {
        .wr_id   = slot,
        .sg_list = &sge,
        .num_sge = 1,
        .next    = NULL,
    };
    struct ibv_recv_wr *bad = NULL;
    return ibv_post_recv(c->qp, &wr, &bad);
}

static rdma_ctx_t *ctx_create(struct rdma_cm_id *id,
                               struct rdma_event_channel *ec) {
    rdma_ctx_t *c = (rdma_ctx_t *)calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->id = id;
    c->ec = ec;

    c->pd = ibv_alloc_pd(id->verbs);
    if (!c->pd) goto fail;

    c->comp_ch = ibv_create_comp_channel(id->verbs);
    if (!c->comp_ch) goto fail;

    // O_NONBLOCK so ctx_drain_events can loop until EAGAIN.
    // ctx_wait_event uses poll() which works regardless of blocking mode.
    {
        int flags = fcntl(c->comp_ch->fd, F_GETFL);
        if (flags < 0 || fcntl(c->comp_ch->fd, F_SETFL, flags | O_NONBLOCK))
            goto fail;
    }

    c->send_cq = ibv_create_cq(id->verbs, RDMA_RECV_DEPTH, NULL, NULL, 0);
    c->recv_cq = ibv_create_cq(id->verbs, RDMA_RECV_DEPTH, NULL, c->comp_ch, 0);
    if (!c->send_cq || !c->recv_cq) goto fail;

    if (ibv_req_notify_cq(c->recv_cq, 0)) goto fail;

    struct ibv_qp_init_attr qa;
    memset(&qa, 0, sizeof(qa));
    qa.send_cq = c->send_cq;
    qa.recv_cq = c->recv_cq;
    qa.qp_type = IBV_QPT_RC;
    qa.cap.max_send_wr     = RDMA_RECV_DEPTH;
    qa.cap.max_recv_wr     = RDMA_RECV_DEPTH;
    qa.cap.max_send_sge    = 1;
    qa.cap.max_recv_sge    = 1;
    qa.cap.max_inline_data = 64;
    if (rdma_create_qp(id, c->pd, &qa)) goto fail;
    c->qp = id->qp;

    c->send_buf = (uint8_t *)malloc(RDMA_FRAG_SIZE);
    c->recv_buf = (uint8_t *)malloc((size_t)RDMA_RECV_DEPTH * RDMA_FRAG_SIZE);
    if (!c->send_buf || !c->recv_buf) goto fail;

    c->send_mr = ibv_reg_mr(c->pd, c->send_buf, RDMA_FRAG_SIZE,
                            IBV_ACCESS_LOCAL_WRITE);
    c->recv_mr = ibv_reg_mr(c->pd, c->recv_buf,
                            (size_t)RDMA_RECV_DEPTH * RDMA_FRAG_SIZE,
                            IBV_ACCESS_LOCAL_WRITE);
    if (!c->send_mr || !c->recv_mr) goto fail;

    for (int i = 0; i < RDMA_RECV_DEPTH; i++)
        if (post_recv_slot(c, i)) goto fail;

    id->context = c;
    return c;

fail:
    if (c->send_mr) ibv_dereg_mr(c->send_mr);
    if (c->recv_mr) ibv_dereg_mr(c->recv_mr);
    free(c->send_buf);
    free(c->recv_buf);
    if (c->qp) ibv_destroy_qp(c->qp);
    if (c->send_cq) ibv_destroy_cq(c->send_cq);
    if (c->recv_cq) ibv_destroy_cq(c->recv_cq);
    if (c->comp_ch) ibv_destroy_comp_channel(c->comp_ch);
    if (c->pd) ibv_dealloc_pd(c->pd);
    free(c);
    return NULL;
}

static void ctx_destroy(rdma_ctx_t *c) {
    if (!c) return;
    rdma_disconnect(c->id);
    if (c->send_mr) ibv_dereg_mr(c->send_mr);
    if (c->recv_mr) ibv_dereg_mr(c->recv_mr);
    free(c->send_buf);
    free(c->recv_buf);
    if (c->qp) ibv_destroy_qp(c->qp);
    if (c->send_cq) ibv_destroy_cq(c->send_cq);
    if (c->recv_cq) ibv_destroy_cq(c->recv_cq);
    if (c->comp_ch) ibv_destroy_comp_channel(c->comp_ch);
    if (c->pd) ibv_dealloc_pd(c->pd);
    rdma_destroy_id(c->id);
    if (c->ec) rdma_destroy_event_channel(c->ec);
    free(c);
}

// ---- send -------------------------------------------------------------------

static int ctx_send_frag(rdma_ctx_t *c,
                         const void *data, int data_len,
                         uint32_t total_len, uint32_t frag_off) {
    uint32_t hdr[2] = {total_len, frag_off};
    memcpy(c->send_buf,                  hdr,  RDMA_HDR_SIZE);
    memcpy(c->send_buf + RDMA_HDR_SIZE, data, data_len);

    int msg_len = data_len + RDMA_HDR_SIZE;
    struct ibv_sge sge = {
        .addr   = (uint64_t)c->send_buf,
        .length = (uint32_t)msg_len,
        .lkey   = c->send_mr->lkey,
    };
    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sge;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(c->qp, &wr, &bad)) return -1;

    struct ibv_wc wc;
    for (;;) {
        int n = ibv_poll_cq(c->send_cq, 1, &wc);
        if (n > 0) {
            if (wc.status == IBV_WC_SUCCESS) return 0;
            errno = EIO;  // QP error; wc.status has the ibv_wc_status code
            return -1;
        }
        if (n < 0) return -1;
    }
}

// ---- recv: poll-based wait with interrupt pipe ------------------------------
//
// ctx_wait_event blocks in poll() on two fds:
//   - comp_ch->fd : NIC completion notification
//   - interrupt_fd: write end of a Go-managed pipe; written by Close()
//
// Returns: 1 = comp event ready, 0 = interrupted (Close called), -1 = error.
//
// This runs in a dedicated CGo goroutine (one OS thread consumed while
// waiting), keeping readLoop goroutine free to park in Go's scheduler.

static int ctx_wait_event(rdma_ctx_t *c, int interrupt_fd) {
    struct pollfd pfds[2];
    pfds[0].fd     = c->comp_ch->fd;
    pfds[0].events = POLLIN;
    pfds[1].fd     = interrupt_fd;
    pfds[1].events = POLLIN;
    int r;
    do { r = poll(pfds, 2, -1); } while (r < 0 && errno == EINTR);
    if (r <= 0) return -1;
    if (pfds[0].revents & POLLIN) return 1;
    return 0;  // interrupt_fd readable -> Close() was called
}

// ctx_drain_events: consume all pending events from comp_ch (non-blocking),
// ack them, and re-arm the CQ.
// Returns: > 0 events processed, 0 EAGAIN (none pending), -1 error.
static int ctx_drain_events(rdma_ctx_t *c) {
    int n = 0;
    for (;;) {
        struct ibv_cq *ev_cq;
        void          *ev_ctx;
        if (ibv_get_cq_event(c->comp_ch, &ev_cq, &ev_ctx)) {
            int e = errno;
            if (e == EAGAIN || e == EWOULDBLOCK) break;
            return -1;
        }
        n++;
    }
    if (n > 0) {
        ibv_ack_cq_events(c->recv_cq, n);
        if (ibv_req_notify_cq(c->recv_cq, 0)) return -1;
    }
    return n;
}

// ---- recv: non-blocking CQ poll --------------------------------------------

static int ctx_poll_recv(rdma_ctx_t *c,
                         void *dst, int dst_max,
                         uint32_t *total_len, uint32_t *frag_off) {
    struct ibv_wc wc;
    int n = ibv_poll_cq(c->recv_cq, 1, &wc);
    if (n == 0) return 0;
    if (n < 0 || wc.status != IBV_WC_SUCCESS) return -1;
    if (wc.opcode != IBV_WC_RECV) return 0;

    uint8_t *slot = c->recv_buf + wc.wr_id * RDMA_FRAG_SIZE;
    uint32_t hdr[2];
    memcpy(hdr, slot, RDMA_HDR_SIZE);
    *total_len = hdr[0];
    *frag_off  = hdr[1];

    int payload = (int)wc.byte_len - RDMA_HDR_SIZE;
    if (payload < 0) { post_recv_slot(c, (uint32_t)wc.wr_id); return -1; }
    int copy = (payload < dst_max) ? payload : dst_max;
    memcpy(dst, slot + RDMA_HDR_SIZE, copy);
    post_recv_slot(c, (uint32_t)wc.wr_id);
    return copy;
}

// ---- address helpers --------------------------------------------------------

static void ctx_local_addr(rdma_ctx_t *c, char *buf, uint16_t *port) {
    struct sockaddr_in *sa = (struct sockaddr_in *)rdma_get_local_addr(c->id);
    inet_ntop(AF_INET, &sa->sin_addr, buf, INET_ADDRSTRLEN);
    *port = ntohs(sa->sin_port);
}

static void ctx_peer_addr(rdma_ctx_t *c, char *buf, uint16_t *port) {
    struct sockaddr_in *sa = (struct sockaddr_in *)rdma_get_peer_addr(c->id);
    inet_ntop(AF_INET, &sa->sin_addr, buf, INET_ADDRSTRLEN);
    *port = ntohs(sa->sin_port);
}

// ---- server listener --------------------------------------------------------

typedef struct {
    struct rdma_event_channel *ec;
    struct rdma_cm_id         *lid;
} rdma_listener_t;

static rdma_listener_t *listener_create(const char *ip, int port) {
    rdma_listener_t *l = (rdma_listener_t *)calloc(1, sizeof(*l));
    if (!l) return NULL;
    l->ec = rdma_create_event_channel();
    if (!l->ec) { free(l); return NULL; }

    if (rdma_create_id(l->ec, &l->lid, NULL, RDMA_PS_TCP)) goto fail;

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &sa.sin_addr) != 1) goto fail;

    if (rdma_bind_addr(l->lid, (struct sockaddr *)&sa)) goto fail;
    if (rdma_listen(l->lid, 16)) goto fail;
    return l;

fail:
    if (l->lid) rdma_destroy_id(l->lid);
    rdma_destroy_event_channel(l->ec);
    free(l);
    return NULL;
}

static rdma_ctx_t *listener_accept(rdma_listener_t *l) {
    struct rdma_cm_event *ev;
    for (;;) {
        if (rdma_get_cm_event(l->ec, &ev)) return NULL;
        if (ev->event == RDMA_CM_EVENT_CONNECT_REQUEST) break;
        rdma_ack_cm_event(ev);
    }

    struct rdma_cm_id *new_id = ev->id;
    rdma_ack_cm_event(ev);

    struct rdma_event_channel *conn_ec = rdma_create_event_channel();
    if (!conn_ec) { rdma_destroy_id(new_id); return NULL; }
    if (rdma_migrate_id(new_id, conn_ec)) {
        rdma_destroy_event_channel(conn_ec);
        rdma_destroy_id(new_id);
        return NULL;
    }

    rdma_ctx_t *ctx = ctx_create(new_id, conn_ec);
    if (!ctx) {
        rdma_destroy_id(new_id);
        rdma_destroy_event_channel(conn_ec);
        return NULL;
    }

    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.responder_resources = 1;
    cp.initiator_depth     = 1;
    cp.retry_count         = 7;  // retry on transport error
    cp.rnr_retry_count     = 7;  // 7 = infinite retry on RNR NAK
    if (rdma_accept(new_id, &cp)) { ctx_destroy(ctx); return NULL; }

    if (rdma_get_cm_event(conn_ec, &ev)) { ctx_destroy(ctx); return NULL; }
    int ok = (ev->event == RDMA_CM_EVENT_ESTABLISHED);
    rdma_ack_cm_event(ev);
    if (!ok) { ctx_destroy(ctx); return NULL; }

    return ctx;
}

static void listener_destroy(rdma_listener_t *l) {
    if (!l) return;
    rdma_destroy_id(l->lid);
    rdma_destroy_event_channel(l->ec);
    free(l);
}

// ---- client connect ---------------------------------------------------------

static rdma_ctx_t *conn_create(const char *ip, int port) {
    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) return NULL;

    struct rdma_cm_id *id = NULL;
    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP)) goto fail_ec;

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &sa.sin_addr) != 1) goto fail_id;

    struct rdma_cm_event *ev;
    if (rdma_resolve_addr(id, NULL, (struct sockaddr *)&sa, 2000)) goto fail_id;
    if (rdma_get_cm_event(ec, &ev)) goto fail_id;
    int ok = (ev->event == RDMA_CM_EVENT_ADDR_RESOLVED);
    rdma_ack_cm_event(ev);
    if (!ok) goto fail_id;

    if (rdma_resolve_route(id, 2000)) goto fail_id;
    if (rdma_get_cm_event(ec, &ev)) goto fail_id;
    ok = (ev->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
    rdma_ack_cm_event(ev);
    if (!ok) goto fail_id;

    rdma_ctx_t *ctx = ctx_create(id, ec);
    if (!ctx) goto fail_id;

    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.responder_resources = 1;
    cp.initiator_depth     = 1;
    cp.retry_count         = 7;  // retry on transport error
    cp.rnr_retry_count     = 7;  // 7 = infinite retry on RNR NAK
    if (rdma_connect(id, &cp)) { ctx_destroy(ctx); return NULL; }

    if (rdma_get_cm_event(ec, &ev)) { ctx_destroy(ctx); return NULL; }
    ok = (ev->event == RDMA_CM_EVENT_ESTABLISHED);
    rdma_ack_cm_event(ev);
    if (!ok) { ctx_destroy(ctx); return NULL; }

    return ctx;

fail_id: rdma_destroy_id(id);
fail_ec: rdma_destroy_event_channel(ec);
    return NULL;
}
*/
import "C"

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// ---- net.Addr ---------------------------------------------------------------

type rdmaAddr struct{ s string }

func (a rdmaAddr) Network() string { return "rdma" }
func (a rdmaAddr) String() string  { return a.s }

// ---- helpers ----------------------------------------------------------------

func cErrno() error {
	e := C.get_errno()
	return fmt.Errorf("errno %d", int(e))
}

func ctxAddrString(ctx *C.rdma_ctx_t, local bool) string {
	var buf [C.INET_ADDRSTRLEN]C.char
	var port C.uint16_t
	if local {
		C.ctx_local_addr(ctx, &buf[0], &port)
	} else {
		C.ctx_peer_addr(ctx, &buf[0], &port)
	}
	return fmt.Sprintf("%s:%d", C.GoString(&buf[0]), int(port))
}

func parseAddr(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	ips, err := net.LookupHost(host)
	if err != nil {
		return "", 0, err
	}
	return ips[0], port, nil
}

// ---- timeoutError -----------------------------------------------------------

type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// ---- rdmaReader (deadline-aware condvar buffer) -----------------------------

type rdmaReader struct {
	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	err      error
	deadline time.Time
}

func newRdmaReader() *rdmaReader {
	r := &rdmaReader{}
	r.cond = sync.NewCond(&r.mu)
	return r
}

func (r *rdmaReader) feed(data []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return false
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	r.buf = append(r.buf, cp...)
	r.cond.Broadcast()
	return true
}

func (r *rdmaReader) setError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
	r.cond.Broadcast()
}

func (r *rdmaReader) Read(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if len(r.buf) > 0 {
			n := copy(b, r.buf)
			r.buf = r.buf[n:]
			return n, nil
		}
		if r.err != nil {
			return 0, r.err
		}
		if !r.deadline.IsZero() && !r.deadline.After(time.Now()) {
			return 0, &timeoutError{}
		}
		r.cond.Wait()
	}
}

func (r *rdmaReader) setDeadline(t time.Time) {
	r.mu.Lock()
	r.deadline = t
	r.mu.Unlock()
	r.cond.Broadcast()
	if !t.IsZero() && t.After(time.Now()) {
		time.AfterFunc(time.Until(t), func() { r.cond.Broadcast() })
	}
}

// ---- net.Conn ---------------------------------------------------------------

const fragBufSize = C.RDMA_FRAG_SIZE - C.RDMA_HDR_SIZE

type rdmaConn struct {
	ctx       *C.rdma_ctx_t
	laddr     rdmaAddr
	raddr     rdmaAddr
	rd        *rdmaReader
	// eventCh carries the number of CQ events drained (> 0), or -1 on error.
	// Buffered so eventWatcher can exit without readLoop consuming immediately.
	eventCh   chan int
	// closeDone is closed by Close() to unblock goroutines waiting on selects.
	closeDone chan struct{}
	// wakeWFd is the write end of an interrupt pipe; Close() writes one byte
	// to unblock poll() in eventWatcher's CGo call.
	wakeWFd   int
	// wg tracks eventWatcher so Close() waits before destroying C resources.
	wg        sync.WaitGroup
}

func newRdmaConn(ctx *C.rdma_ctx_t) *rdmaConn {
	// Create a self-pipe so Close() can interrupt ctx_wait_event's poll().
	var pipeFds [2]int
	if err := syscall.Pipe(pipeFds[:]); err != nil {
		panic(fmt.Sprintf("rdma: pipe: %v", err))
	}
	// pipeFds[0] = read end (passed to eventWatcher / C poll)
	// pipeFds[1] = write end (written by Close)

	c := &rdmaConn{
		ctx:       ctx,
		laddr:     rdmaAddr{ctxAddrString(ctx, true)},
		raddr:     rdmaAddr{ctxAddrString(ctx, false)},
		rd:        newRdmaReader(),
		eventCh:   make(chan int, 1),
		closeDone: make(chan struct{}),
		wakeWFd:   pipeFds[1],
	}
	c.wg.Add(1)
	go c.eventWatcher(pipeFds[0])
	go c.readLoop()
	return c
}

// eventWatcher is a dedicated goroutine that blocks in poll() via CGo.
// It consumes one OS thread while waiting, but that is unavoidable without
// Go 1.12's SyscallConn.  Crucially, readLoop does NOT block in CGo —
// it parks in Go's scheduler waiting on eventCh.
func (c *rdmaConn) eventWatcher(wakeRFd int) {
	defer c.wg.Done()
	defer syscall.Close(wakeRFd)

	for {
		// Blocks in poll() — one OS thread consumed here, not in readLoop.
		r := C.ctx_wait_event(c.ctx, C.int(wakeRFd))
		if r <= 0 {
			// 0 = wakeRFd readable (Close called), -1 = poll error.
			select {
			case c.eventCh <- -1:
			case <-c.closeDone:
			}
			return
		}
		// Drain all pending CQ events non-blocking (comp_ch->fd is O_NONBLOCK).
		n := C.ctx_drain_events(c.ctx)
		if n < 0 {
			select {
			case c.eventCh <- -1:
			case <-c.closeDone:
			}
			return
		}
		if n > 0 {
			select {
			case c.eventCh <- int(n):
			case <-c.closeDone:
				return
			}
		}
		// n == 0 (EAGAIN after wait) is unexpected but harmless — loop again.
	}
}

// readLoop parks in Go's scheduler waiting on eventCh; zero OS threads consumed.
func (c *rdmaConn) readLoop() {
	fragBuf := make([]byte, fragBufSize)
	var assemblyBuf   []byte
	var assemblyTotal int

	for {
		// Park goroutine in Go's scheduler — no CGo, no OS thread held here.
		var n int
		select {
		case n = <-c.eventCh:
		case <-c.closeDone:
			return
		}
		if n <= 0 {
			if n < 0 {
				c.rd.setError(fmt.Errorf("rdma: recv event failed"))
			}
			return
		}

		// Drain all pending recv completions — pure user-space CQ reads.
		for {
			var totalLen, fragOff C.uint32_t
			got := C.ctx_poll_recv(c.ctx,
				unsafe.Pointer(&fragBuf[0]), C.int(len(fragBuf)),
				&totalLen, &fragOff)
			if got == 0 {
				break
			}
			if got < 0 {
				c.rd.setError(fmt.Errorf("rdma: poll recv failed"))
				return
			}

			total := int(totalLen)
			off := int(fragOff)

			if off == 0 {
				assemblyBuf = make([]byte, total)
				assemblyTotal = total
			}
			copy(assemblyBuf[off:], fragBuf[:int(got)])
			if off+int(got) >= assemblyTotal {
				if !c.rd.feed(assemblyBuf) {
					return
				}
				assemblyBuf = nil
			}
		}
	}
}

func (c *rdmaConn) Read(b []byte) (int, error) {
	return c.rd.Read(b)
}

func (c *rdmaConn) Write(b []byte) (int, error) {
	total := len(b)
	offset := 0
	for offset < total {
		end := offset + int(fragBufSize)
		if end > total {
			end = total
		}
		chunk := b[offset:end]
		r := C.ctx_send_frag(c.ctx,
			unsafe.Pointer(&chunk[0]), C.int(len(chunk)),
			C.uint32_t(total), C.uint32_t(offset))
		if r < 0 {
			return offset, fmt.Errorf("rdma: send failed: %v", cErrno())
		}
		offset = end
	}
	return total, nil
}

// Close signals both goroutines to stop, waits for eventWatcher to finish
// with all CGo resources, then destroys the ibverbs context.
func (c *rdmaConn) Close() error {
	c.rd.setError(io.ErrClosedPipe)
	close(c.closeDone)                       // unblock select in readLoop and eventWatcher
	syscall.Write(c.wakeWFd, []byte{0})      // unblock poll() in eventWatcher CGo call
	syscall.Close(c.wakeWFd)
	c.wg.Wait()                              // ensure eventWatcher is done with C resources
	C.ctx_destroy(c.ctx)
	return nil
}

func (c *rdmaConn) LocalAddr() net.Addr  { return c.laddr }
func (c *rdmaConn) RemoteAddr() net.Addr { return c.raddr }

func (c *rdmaConn) SetDeadline(t time.Time) error {
	c.rd.setDeadline(t)
	return nil
}

func (c *rdmaConn) SetReadDeadline(t time.Time) error {
	c.rd.setDeadline(t)
	return nil
}

func (c *rdmaConn) SetWriteDeadline(t time.Time) error { return nil }

// ---- net.Listener -----------------------------------------------------------

type rdmaListener struct {
	l    *C.rdma_listener_t
	addr rdmaAddr
}

type rdmaAcceptError struct{ err error }

func (e *rdmaAcceptError) Error() string   { return e.err.Error() }
func (e *rdmaAcceptError) Timeout() bool   { return false }
func (e *rdmaAcceptError) Temporary() bool { return true }

func (l *rdmaListener) Accept() (net.Conn, error) {
	ctx := C.listener_accept(l.l)
	if ctx == nil {
		return nil, &rdmaAcceptError{fmt.Errorf("rdma: accept failed: %v", cErrno())}
	}
	return newRdmaConn(ctx), nil
}

func (l *rdmaListener) Close() error {
	C.listener_destroy(l.l)
	return nil
}

func (l *rdmaListener) Addr() net.Addr { return l.addr }

// ---- public API -------------------------------------------------------------

func Listen(addr string) (net.Listener, error) {
	host, port, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	l := C.listener_create(cHost, C.int(port))
	if l == nil {
		return nil, fmt.Errorf("rdma: listen %s failed: %v", addr, cErrno())
	}
	return &rdmaListener{l: l, addr: rdmaAddr{addr}}, nil
}

func Dial(addr string) (net.Conn, error) {
	host, port, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	ctx := C.conn_create(cHost, C.int(port))
	if ctx == nil {
		return nil, fmt.Errorf("rdma: connect %s failed: %v", addr, cErrno())
	}
	return newRdmaConn(ctx), nil
}
