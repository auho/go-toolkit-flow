package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit/time/timing"
)

const (
	StatusConfig  = "config"
	StatusAccept  = "accept"
	StatusScan    = "scan"
	StatusDone    = "done"
	StatusFinish  = "finish"
)

type StateProvider interface {
	GetStatus() string
	Overview() string
}

// baseState 基状态
type baseState struct {
	Concurrency int
	Title       string
	amount      int64
	duration    timing.Duration
	status      string
}

func (s *baseState) Status() string {
	return s.status
}

func (s *baseState) Amount() int64 {
	return s.amount
}

func (s *baseState) SetAmount(n int64) {
	atomic.StoreInt64(&s.amount, n)
}

func (s *baseState) AddAmount(n int64) {
	atomic.AddInt64(&s.amount, n)
}

func (s *baseState) SetStatus(status string) {
	s.status = status
}

func (s *baseState) MarkAsConfigured() {
	s.status = StatusConfig
}

func (s *baseState) MarkAsAccepted() {
	s.status = StatusAccept
}

func (s *baseState) MarkAsScanning() {
	s.status = StatusScan
}

func (s *baseState) MarkAsDone() {
	s.status = StatusDone
}

func (s *baseState) MarkAsFinished() {
	s.status = StatusFinish
}

func (s *baseState) DurationStart() {
	s.duration.Start()
}

func (s *baseState) DurationStop() {
	s.duration.Stop()
}

// State 状态
type State struct {
	baseState
}

func NewState() *State {
	return &State{}
}

func (s *State) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d, Duration: %s",
		s.status,
		s.Concurrency,
		s.amount,
		s.duration.StringStartToStop())
}

// TotalState 总数状态
type TotalState struct {
	baseState
	Total int64
}

func NewTotalState() *TotalState {
	return &TotalState{}
}

func (t *TotalState) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d/%d, Duration: %s",
		t.status,
		t.Concurrency,
		t.amount,
		t.Total,
		t.duration.StringStartToStop())
}

// PageState 分页状态
type PageState struct {
	baseState
	Page      int64
	PageSize  int64
	TotalPage int64
	Total     int64
}

func NewPageState() *PageState {
	return &PageState{}
}

func (p *PageState) Overview() string {
	return fmt.Sprintf("Status: %s, Concurrency: %d, Amount: %d/%d, Page: %d/%d(%d), Duration: %s",
		p.status,
		p.Concurrency,
		p.amount,
		p.Total,
		p.Page,
		p.TotalPage,
		p.PageSize,
		p.duration.StringStartToStop())
}
