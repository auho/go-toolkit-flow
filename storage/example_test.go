package storage_test

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

func ExampleStateInfo() {
	s := storage.NewStateInfo()
	s.SetTitle("my-source")
	s.MarkAsScanning()
	s.SetAmount(42)
	s.SetConcurrency(2)
	s.DurationStart()
	s.DurationStop()
	fmt.Println(s.Overview())
	// Output:
	// Status: scan, Concurrency: 2, Amount: 42, Duration: 0.000000 秒
}

func ExampleTotalStateInfo() {
	s := storage.NewTotalState()
	s.SetTitle("my-source")
	s.MarkAsAccepted()
	s.SetAmount(50)
	s.SetTotal(100)
	s.SetConcurrency(4)
	s.DurationStart()
	s.DurationStop()
	fmt.Println(s.Overview())
	// Output:
	// Status: accept, Concurrency: 4, Amount: 50/100, Duration: 0.000000 秒
}

func ExamplePageStateInfo() {
	s := storage.NewPageState()
	s.SetTitle("my-source")
	s.MarkAsScanning()
	s.SetAmount(50)
	s.SetTotal(100)
	s.SetPageSize(10)
	s.SetTotalPage(10)
	s.AddPage(5)
	s.SetConcurrency(2)
	s.DurationStart()
	s.DurationStop()
	fmt.Println(s.Overview())
	// Output:
	// Status: scan, Concurrency: 2, Amount: 50/100, Page: 5/10(10), Duration: 0.000000 秒
}

func ExampleMultiState() {
	s1 := storage.NewStateInfo()
	s1.SetTitle("source")
	s1.SetAmount(100)

	s2 := storage.NewStateInfo()
	s2.SetTitle("dest")
	s2.SetAmount(50)

	ms := storage.NewMultiState([]storage.State{s1, s2})
	fmt.Println(ms.Amount())
	// Output:
	// 150
}

func ExampleNoopDestination() {
	var d storage.NoopDestination[string]

	_ = d.Prepare(context.Background())
	d.Accept()
	_ = d.Receive([]string{"a", "b"})
	d.Done()
	_ = d.Finish()
	_ = d.Close()
	fmt.Println("ok")
	// Output:
	// ok
}

// spyDest is a simple Destination[string] for MultiDestination example.
type spyDest struct {
	name string
}

func (s *spyDest) Prepare(_ context.Context) error { return nil }
func (s *spyDest) Accept()                          {}
func (s *spyDest) Receive(_ []string) error         { return nil }
func (s *spyDest) Done()                            {}
func (s *spyDest) Finish() error                    { return nil }
func (s *spyDest) Close() error                     { return nil }
func (s *spyDest) Summary() []string                { return []string{s.name} }
func (s *spyDest) StateInfo() storage.State {
	st := storage.NewStateInfo()
	st.SetTitle(s.name)
	return st
}
func (s *spyDest) StateString() []string { return []string{s.StateInfo().Overview()} }

func ExampleMultiDestination() {
	d1 := &spyDest{name: "alpha"}
	d2 := &spyDest{name: "beta"}
	md := storage.MultiDestination[string]{d1, d2}

	_ = md.Prepare(context.Background())
	md.Accept()
	_ = md.Receive([]string{"a", "b", "c"})
	md.Done()
	_ = md.Finish()
	_ = md.Close()

	for _, s := range md.Summary() {
		fmt.Println(s)
	}
	// Output:
	// alpha
	// beta
}