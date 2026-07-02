package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit/v2/time/timing"
)

// Status constants for the state machine.
const (
	StatusConfig  = "config"
	StatusPrepare = "prepare"
	StatusAccept  = "accept"
	StatusScan    = "scan"
	StatusDone    = "done"
	StatusFinish  = "finish"
)

// State is the base state info interface for all sources and destinations.
// It provides structured access to runtime state for external consumers.
type State interface {
	Overview() string
	Amount() int64
	Title() string
	Concurrency() int
}

// TotalState extends State with total tracking.
// Sources that know the total number of items implement this.
type TotalState interface {
	State
	Total() int64
}

// PageState extends TotalState with pagination tracking.
type PageState interface {
	TotalState
	Page() int64
	PageSize() int64
	TotalPage() int64
}

// baseState provides concurrent-safe state tracking for Source and Destination
// implementations. It tracks status, amount, and duration using atomic operations.
type baseState struct {
	concurrency int
	title       string
	amount      int64
	duration    timing.Duration
	status      atomic.Value
}

func (s *baseState) Status() string {
	v := s.status.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

func (s *baseState) Amount() int64 {
	return atomic.LoadInt64(&s.amount)
}

func (s *baseState) SetAmount(n int64) {
	atomic.StoreInt64(&s.amount, n)
}

func (s *baseState) AddAmount(n int64) {
	atomic.AddInt64(&s.amount, n)
}

func (s *baseState) SetStatus(status string) {
	s.status.Store(status)
}

func (s *baseState) Title() string {
	return s.title
}

func (s *baseState) SetTitle(title string) {
	s.title = title
}

func (s *baseState) Concurrency() int {
	return s.concurrency
}

func (s *baseState) SetConcurrency(n int) {
	s.concurrency = n
}

// MarkAsConfigured sets the status to StatusConfig.
func (s *baseState) MarkAsConfigured() {
	s.SetStatus(StatusConfig)
}

// MarkAsPrepare sets the status to StatusPrepare.
func (s *baseState) MarkAsPrepare() {
	s.SetStatus(StatusPrepare)
}

// MarkAsAccepted sets the status to StatusAccept.
func (s *baseState) MarkAsAccepted() {
	s.SetStatus(StatusAccept)
}

// MarkAsScanning sets the status to StatusScan.
func (s *baseState) MarkAsScanning() {
	s.SetStatus(StatusScan)
}

// MarkAsDone sets the status to StatusDone.
func (s *baseState) MarkAsDone() {
	s.SetStatus(StatusDone)
}

// MarkAsFinished sets the status to StatusFinish.
func (s *baseState) MarkAsFinished() {
	s.SetStatus(StatusFinish)
}

// DurationStart starts the duration timer.
func (s *baseState) DurationStart() {
	s.duration.Start()
}

// DurationStop stops the duration timer.
func (s *baseState) DurationStop() {
	s.duration.Stop()
}

// Snapshot is a basic state tracker with status, amount, and duration.
// Suitable for destinations that do not need pagination or total tracking.
type Snapshot struct {
	baseState
}

// NewSnapshot creates a new Snapshot.
func NewSnapshot() *Snapshot {
	return &Snapshot{}
}

// Overview returns a formatted string summarizing the current state.
func (s *Snapshot) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d, Duration: %s",
		s.Status(),
		s.Concurrency(),
		s.Amount(),
		s.duration.StringStartToStop())
}

// TotalSnapshot extends Snapshot with a Total field for tracking progress against
// a known total. Suitable for sources that know the total number of items.
type TotalSnapshot struct {
	baseState
	total int64
}

// NewTotalSnapshot creates a new TotalSnapshot.
func NewTotalSnapshot() *TotalSnapshot {
	return &TotalSnapshot{}
}

func (t *TotalSnapshot) Total() int64 {
	return t.total
}

func (t *TotalSnapshot) SetTotal(n int64) {
	t.total = n
}

// Overview returns a formatted string summarizing the current state with
// progress (Amount/Total).
func (t *TotalSnapshot) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d/%d, Duration: %s",
		t.Status(),
		t.Concurrency(),
		t.Amount(),
		t.Total(),
		t.duration.StringStartToStop())
}

// PageSnapshot extends TotalSnapshot with pagination tracking (Page, PageSize,
// TotalPage). Suitable for paged database sources.
type PageSnapshot struct {
	baseState
	page      int64
	pageSize  int64
	totalPage int64
	total     int64
}

// NewPageSnapshot creates a new PageSnapshot.
func NewPageSnapshot() *PageSnapshot {
	return &PageSnapshot{}
}

func (p *PageSnapshot) Page() int64 {
	return atomic.LoadInt64(&p.page)
}

// AddPage atomically increments the page counter by n.
func (p *PageSnapshot) AddPage(n int64) {
	atomic.AddInt64(&p.page, n)
}

func (p *PageSnapshot) PageSize() int64 {
	return p.pageSize
}

func (p *PageSnapshot) SetPageSize(n int64) {
	p.pageSize = n
}

func (p *PageSnapshot) TotalPage() int64 {
	return p.totalPage
}

func (p *PageSnapshot) SetTotalPage(n int64) {
	p.totalPage = n
}

func (p *PageSnapshot) Total() int64 {
	return p.total
}

func (p *PageSnapshot) SetTotal(n int64) {
	p.total = n
}

// Overview returns a formatted string summarizing the current state with
// pagination progress (Page/TotalPage, Amount/Total).
func (p *PageSnapshot) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d/%d, Page: %d/%d(%d), Duration: %s",
		p.Status(),
		p.Concurrency(),
		p.Amount(),
		p.Total(),
		p.Page(),
		p.TotalPage(),
		p.PageSize(),
		p.duration.StringStartToStop())
}

// Compile-time interface conformance checks.
var (
	_ State      = (*Snapshot)(nil)
	_ TotalState = (*TotalSnapshot)(nil)
	_ PageState  = (*PageSnapshot)(nil)
	_ TotalState = (*PageSnapshot)(nil)
	_ State      = (*TotalSnapshot)(nil)
	_ State      = (*PageSnapshot)(nil)
)
