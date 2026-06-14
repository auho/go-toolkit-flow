package source

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect"
	"github.com/auho/go-toolkit-flow/storage/database/source/format"
	"golang.org/x/sync/errgroup"
)

// ScanConfig 类型别名重导出，用户无需导入 dialect 包
type ScanConfig = dialect.ScanConfig

var _ storage.Source[storage.MapEntry] = (*Section[storage.MapEntry])(nil)

// Section 分段查询编排器
type Section[E storage.Entry] struct {
	storage.Storage
	dialect dialect.Dialect
	format  format.Format[E]
	config  SectionConfig

	total     int64
	totalPage int64
	startID   int64
	endID     int64

	rowsChan    chan []E
	segmentChan chan []int64
	state       *storage.PageState

	// 并发与错误处理
	segmentCtx    context.Context
	segmentCancel context.CancelFunc
	scanGroup     *errgroup.Group
	scanCtx       context.Context
	scanCancel    context.CancelFunc
	scanError     error
}

func newSection[E storage.Entry](c SectionConfig, d dialect.Dialect, f format.Format[E]) *Section[E] {
	s := &Section[E]{
		dialect: d,
		format:  f,
		config:  c,
	}

	s.initConfig(c)

	return s
}

func (s *Section[E]) DB() *database.DB {
	if driver, ok := s.dialect.(database.Driver); ok {
		return driver.DB()
	}

	return nil
}

func (s *Section[E]) Copy(items []E) []E {
	return s.format.Copy(items)
}

func (s *Section[E]) Scan() error {
	s.state.MarkAsScanning()
	s.state.DurationStart()

	s.rowsChan = make(chan []E, s.config.Concurrency)
	s.segmentChan = make(chan []int64, s.config.Concurrency)

	s.segmentCtx, s.segmentCancel = context.WithCancel(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	s.scanGroup, s.scanCtx = errgroup.WithContext(ctx)
	s.scanCancel = cancel

	err := s.idRange()
	if err != nil {
		return err
	}

	go s.dispatchSegments()
	s.scanRows()

	go func() {
		s.scanError = s.scanGroup.Wait()

		s.segmentCancel()
		s.scanCancel()

		close(s.rowsChan)

		s.state.DurationStop()
		s.state.MarkAsFinished()
	}()

	return nil
}

func (s *Section[E]) ReceiveChan() <-chan []E {
	return s.rowsChan
}

func (s *Section[E]) Err() error {
	return s.scanError
}

// dispatchSegments 根据 start id, end id 分段并分发
func (s *Section[E]) dispatchSegments() {
	defer close(s.segmentChan)

	startID := s.startID

	for {
		rightID := startID + s.config.PageSize - 1
		if rightID >= s.endID {
			rightID = s.endID
		}

		select {
		case <-s.segmentCtx.Done():
			return
		case s.segmentChan <- []int64{startID, rightID}:
		}

		if rightID >= s.endID {
			break
		}

		startID += s.config.PageSize
	}
}

// scanRows 从 segmentChan 读取分段信息并查询数据
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

					rows, err := s.format.QueryByRange(s.dialect, segment[0], segment[1])
					if err != nil {
						s.scanCancel()

						return fmt.Errorf("query range [%d-%d]: %w", segment[0], segment[1], err)
					}

					if len(rows) > 0 {
						atomic.AddInt64(&s.state.Page, 1)
						s.state.AddAmount(int64(len(rows)))

						s.rowsChan <- rows
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
	s.state.Concurrency = s.config.Concurrency
	s.state.Title = s.Title()
	s.state.MarkAsConfigured()
}

// idRange 查询 ID 边界并计算分页信息
func (s *Section[E]) idRange() error {
	if s.config.PageSize <= 0 {
		return fmt.Errorf("page size[%d] is error", s.config.PageSize)
	}

	minID, maxID, err := s.dialect.FetchIDBounds()
	if err != nil {
		return err
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

	s.state.PageSize = s.config.PageSize
	s.state.TotalPage = s.totalPage
	s.state.Total = s.total

	return nil
}

func (s *Section[E]) Title() string {
	return fmt.Sprintf("Source db[%s]", s.dialect.DBName())
}

func (s *Section[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d, total page: %d, page size: %d, start id: %d, end id: %d ",
		s.Title(),
		s.total,
		s.totalPage,
		s.config.PageSize,
		s.startID,
		s.endID)}
}

func (s *Section[E]) State() []string {
	return []string{s.state.Overview()}
}

func (s *Section[E]) Close() error {
	return s.dialect.Close()
}
