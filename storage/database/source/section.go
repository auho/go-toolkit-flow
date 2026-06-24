// Package source defines database source implementations for reading data.
// It wires together Dialect → Format → Section and manages the lifecycle:
// Prepare → Scan → Finish → Close.
package source

import (
	"context"
	"fmt"
	"math"
	"runtime"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/source/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/database/source/format"
	"golang.org/x/sync/errgroup"
)

// ScanConfig is a type alias re-exported so callers need not import the dialect package.
type ScanConfig = dialect.ScanConfig

var _ storage.Source[storage.MapEntry] = (*Section[storage.MapEntry])(nil)

// Section is a segmented query orchestrator that scans a database table by
// splitting the ID range into fixed-size pages and fetching them concurrently.
type Section[E storage.Entry] struct {
	storage.Storage
	dialect dialect.Dialect
	format  format.Format[E]
	config  SectionConfig

	total     int64
	totalPage int64
	startID   int64
	endID     int64

	itemsChan   chan []E
	segmentChan chan []int64
	state       *storage.PageStateInfo

	// concurrency and error handling
	scanGroup *errgroup.Group
	scanCtx   context.Context
	scanErr   error
}

func newSection[E storage.Entry](f format.Format[E], d dialect.Dialect, c SectionConfig) *Section[E] {
	s := &Section[E]{
		dialect: d,
		format:  f,
		config:  c,
	}

	s.initConfig(c)

	return s
}

func (s *Section[E]) Copy(items []E) []E {
	return s.format.Copy(items)
}

func (s *Section[E]) Prepare(ctx context.Context) error {
	s.state.MarkAsPrepare()

	err := s.idRange()
	if err != nil {
		return fmt.Errorf("idRange: %w", err)
	}

	s.segmentChan = make(chan []int64, s.config.Concurrency)
	s.itemsChan = make(chan []E, s.config.Concurrency)
	s.scanGroup, s.scanCtx = errgroup.WithContext(ctx)

	return nil
}

func (s *Section[E]) Scan() {
	s.state.MarkAsScanning()
	s.state.DurationStart()

	go s.dispatchSegments()
	s.scanRows()
}

func (s *Section[E]) ReceiveChan() <-chan []E {
	return s.itemsChan
}

func (s *Section[E]) Finish() error {
	err := s.scanGroup.Wait()

	close(s.itemsChan)
	s.state.DurationStop()
	s.state.MarkAsFinished()

	return err
}

// dispatchSegments splits the [startID, endID] range into PageSize-sized
// segments and sends them to segmentChan.
// Concurrency model:
//   - Runs in a single goroutine launched by Scan
//   - Sends are cancelled when scanCtx is done
//   - Closes segmentChan on exit
func (s *Section[E]) dispatchSegments() {
	defer close(s.segmentChan)

	startID := s.startID

	for {
		rightID := startID + s.config.PageSize - 1
		if rightID >= s.endID {
			rightID = s.endID
		}

		select {
		case <-s.scanCtx.Done():
			return
		case s.segmentChan <- []int64{startID, rightID}:
		}

		if rightID >= s.endID {
			break
		}

		startID += s.config.PageSize
	}
}

// scanRows reads segment ranges from segmentChan and queries data for each.
// Concurrency model:
//   - Spawns Concurrency worker goroutines via scanGroup
//   - Each worker ranges over segmentChan until it is closed
//   - On error, the worker returns and errgroup cancels scanCtx
func (s *Section[E]) scanRows() {
	for i := 0; i < s.config.Concurrency; i++ {
		s.scanGroup.Go(func() error {
			for {
				select {
				case <-s.scanCtx.Done():
					return nil
				case segment, ok := <-s.segmentChan:
					if !ok {
						return nil
					}

					items, err := s.format.QueryByRange(s.dialect, segment[0], segment[1])
					if err != nil {
						return fmt.Errorf("format.QueryByRange [%d-%d]: %w", segment[0], segment[1], err)
					}

					if len(items) > 0 {
						s.state.AddPage(1)
						s.state.AddAmount(int64(len(items)))

						select {
						case s.itemsChan <- items:
						case <-s.scanCtx.Done():
							return nil
						}
					}
				}
			}
		})
	}
}

func (s *Section[E]) initConfig(config SectionConfig) {
	s.config = config

	s.total = config.MaxItems
	s.startID = config.StartID
	s.endID = config.EndID

	if s.config.Concurrency <= 0 {
		s.config.Concurrency = runtime.NumCPU()
	}

	s.state = storage.NewPageState()
	s.state.SetConcurrency(s.config.Concurrency)
	s.state.SetTitle(s.title())
	s.state.MarkAsConfigured()
}

// idRange queries the ID bounds from the dialect and computes pagination info.
func (s *Section[E]) idRange() error {
	if s.config.PageSize <= 0 {
		return fmt.Errorf("page size[%d] is error", s.config.PageSize)
	}

	minID, maxID, err := s.dialect.FetchIDBounds()
	if err != nil {
		return fmt.Errorf("dialect.FetchIDBounds: %w", err)
	}

	if minID > s.startID {
		s.startID = minID
	}

	if s.endID <= 0 || s.endID > maxID {
		s.endID = maxID
	}

	if s.endID < s.startID {
		return fmt.Errorf("max id %d < start id %d", s.endID, s.startID)
	}

	total := s.endID - s.startID + 1
	if s.total == 0 {
		s.total = total
	} else {
		if s.total < total {
			s.endID = s.startID + s.total - 1
		} else if s.total > total {
			s.total = total
		}
	}

	if s.config.PageSize > s.total {
		s.config.PageSize = s.total
	}

	s.totalPage = int64(math.Ceil(float64(s.total) / float64(s.config.PageSize)))

	s.state.SetPageSize(s.config.PageSize)
	s.state.SetTotalPage(s.totalPage)
	s.state.SetTotal(s.total)

	return nil
}

func (s *Section[E]) title() string {
	return fmt.Sprintf("Source db[%s]", s.dialect.DBName())
}

func (s *Section[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d, total page: %d, page size: %d, start id: %d, end id: %d ",
		s.title(),
		s.total,
		s.totalPage,
		s.config.PageSize,
		s.startID,
		s.endID)}
}

func (s *Section[E]) State() storage.State {
	return s.state
}

func (s *Section[E]) StateString() []string {
	return []string{s.state.Overview()}
}

func (s *Section[E]) Close() error {
	return s.dialect.Close()
}
