package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit/time/timing"
)

const (
	StatusConfig = "config"
	StatusAccept = "accept"
	StatusScan   = "scan"
	StatusDone   = "done"
	StatusFinish = "finish"
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

func (s *baseState) MarkAsConfigured() {
	s.SetStatus(StatusConfig)
}

func (s *baseState) MarkAsAccepted() {
	s.SetStatus(StatusAccept)
}

func (s *baseState) MarkAsScanning() {
	s.SetStatus(StatusScan)
}

func (s *baseState) MarkAsDone() {
	s.SetStatus(StatusDone)
}

func (s *baseState) MarkAsFinished() {
	s.SetStatus(StatusFinish)
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
		s.Status(),
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
		t.Status(),
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

func (p *PageState) GetPage() int64 {
	return atomic.LoadInt64(&p.Page)
}

func (p *PageState) AddPage(n int64) {
	atomic.AddInt64(&p.Page, n)
}

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
