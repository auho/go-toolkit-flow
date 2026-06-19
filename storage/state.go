package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit/time/timing"
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

// StateProvider is the interface for types that expose status and overview.
type StateProvider interface {
	GetStatus() string
	Overview() string
}

// baseState provides concurrent-safe state tracking for Source and Destination
// implementations. It tracks status, amount, and duration using atomic operations.
type baseState struct {
	Concurrency int
	Title       string
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

// State is a basic state tracker with status, amount, and duration.
// Suitable for destinations that do not need pagination or total tracking.
type State struct {
	baseState
}

// NewState creates a new State.
func NewState() *State {
	return &State{}
}

// Overview returns a formatted string summarizing the current state.
func (s *State) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d, Duration: %s",
		s.Status(),
		s.Concurrency,
		s.Amount(),
		s.duration.StringStartToStop())
}

// TotalState extends State with a Total field for tracking progress against
// a known total. Suitable for sources that know the total number of items.
type TotalState struct {
	baseState
	Total int64
}

// NewTotalState creates a new TotalState.
func NewTotalState() *TotalState {
	return &TotalState{}
}

// Overview returns a formatted string summarizing the current state with
// progress (Amount/Total).
func (t *TotalState) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d/%d, Duration: %s",
		t.Status(),
		t.Concurrency,
		t.Amount(),
		t.Total,
		t.duration.StringStartToStop())
}

// PageState extends TotalState with pagination tracking (Page, PageSize,
// TotalPage). Suitable for paged database sources.
type PageState struct {
	baseState
	Page      int64
	PageSize  int64
	TotalPage int64
	Total     int64
}

// NewPageState creates a new PageState.
func NewPageState() *PageState {
	return &PageState{}
}

// GetPage returns the current page number (atomic-safe).
func (p *PageState) GetPage() int64 {
	return atomic.LoadInt64(&p.Page)
}

// AddPage atomically increments the page counter by n.
func (p *PageState) AddPage(n int64) {
	atomic.AddInt64(&p.Page, n)
}

// Overview returns a formatted string summarizing the current state with
// pagination progress (Page/TotalPage, Amount/Total).
func (p *PageState) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d/%d, Page: %d/%d(%d), Duration: %s",
		p.Status(),
		p.Concurrency,
		p.Amount(),
		p.Total,
		p.GetPage(),
		p.TotalPage,
		p.PageSize,
		p.duration.StringStartToStop())
}
