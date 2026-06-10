package source

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
)

var _ storage.Source[storage.MapEntry] = (*Section[storage.MapEntry])(nil)
var _ database.Driver = (*Section[storage.MapEntry])(nil)

type sectionQuery[E storage.Entry] interface {
	Query(se *Section[E], startID, endID int64) ([]E, error)
	Copy([]E) []E
}

// Section 分段查询
type Section[E storage.Entry] struct {
	storage.Storage
	db     *database.DB
	scanWg sync.WaitGroup
	config *QueryConfig

	total         int64
	totalPage     int64
	startID       int64 // 闭区间
	endID         int64 // 闭区间

	failureLastID []int
	idRangeChan   chan []int64 // []in64{left id, right id} 包含两端
	rowsChan      chan []E
	state         *storage.PageState
	query         sectionQuery[E]
}

func newSection[E storage.Entry](config *QueryConfig, sq sectionQuery[E], b database.BuildDb) (*Section[E], error) {
	s := &Section[E]{}
	err := s.initConfig(config, b)
	if err != nil {
		return nil, err
	}

	s.query = sq

	return s, nil
}

func (s *Section[E]) DB() *database.DB {
	return s.db
}

func (s *Section[E]) State() []string {
	return []string{s.state.Overview()}
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

func (s *Section[E]) Copy(items []E) []E {
	return s.query.Copy(items)
}

func (s *Section[E]) Scan() error {
	s.state.MarkAsScanning()
	s.state.DurationStart()
	s.idRangeChan = make(chan []int64, s.config.Concurrency)
	s.rowsChan = make(chan []E, s.config.Concurrency)

	err := s.idRange()
	if err != nil {
		return err
	}

	go s.idSection()
	s.scanRows()

	go func() {
		s.scanWg.Wait()
		close(s.rowsChan)

		s.state.DurationStop()
		s.state.MarkAsFinished()
	}()

	return nil
}

func (s *Section[E]) ReceiveChan() <-chan []E {
	return s.rowsChan
}

// 根据 start id， end id，分段（left id， right id）并分发 id section
func (s *Section[E]) idSection() {
	stopped := false
	startID := s.startID
	rightID := int64(0)
	for {
		rightID = startID + s.config.PageSize - 1
		if rightID >= s.endID {
			rightID = s.endID
			stopped = true
		}

		s.idRangeChan <- []int64{startID, rightID}

		if stopped {
			break
		}

		startID += s.config.PageSize
	}

	close(s.idRangeChan)
}

// 根据 id section（left id，right id） 查询 rows
func (s *Section[E]) scanRows() {
	for i := 0; i < s.config.Concurrency; i++ {
		s.scanWg.Add(1)
		go func() {
			for idRange := range s.idRangeChan {
				atomic.AddInt64(&s.state.Page, 1)

				leftID := idRange[0]
				rightID := idRange[1]

				rows, err := s.query.Query(s, leftID, rightID)
				if err != nil {
					s.LogFatalWithTitle(fmt.Sprintf("left id[%d] - right id[%d]", leftID, rightID), err)
				}

				if len(rows) == 0 {
					continue
				}

				s.state.AddAmount(int64(len(rows)))

				s.rowsChan <- rows
			}

			s.scanWg.Done()
		}()
	}
}

func (s *Section[E]) initConfig(config *QueryConfig, b database.BuildDb) (err error) {
	s.config = config

	s.total = config.Maximum
	s.startID = config.StartID
	s.endID = config.EndID

	s.db, err = b()
	if err != nil {
		return
	}

	err = s.db.Ping()
	if err != nil {
		return
	}

	if s.config.Concurrency <= 0 {
		s.config.Concurrency = runtime.NumCPU()
	}

	if s.config.PageSize <= 0 {
		err = fmt.Errorf("page size[%d] is error", s.config.PageSize)
		return
	}

	if s.total > 0 && s.config.PageSize > s.total {
		s.config.PageSize = s.total
	}

	s.state = storage.NewPageState()
	s.state.Concurrency = s.config.Concurrency
	s.state.Title = s.Title()
	s.state.MarkAsConfigured()

	return
}

// id range
func (s *Section[E]) idRange() error {
	var row struct {
		Max int64
		Min int64
	}

	query := fmt.Sprintf("MAX(%s) AS max, MIN(%s) AS min", s.config.IDName, s.config.IDName)
	err := s.db.Table(s.config.TableName).Select(query).Scan(&row).Error
	if err != nil {
		return fmt.Errorf("id range %w", err)
	}

	if row.Min > s.startID {
		s.startID = row.Min
	}

	if s.endID <= 0 || s.endID > row.Max {
		s.endID = row.Max
	}

	if s.endID < s.startID {
		return fmt.Errorf("mysql max id %d < start id %d", s.endID, s.startID)
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
	return fmt.Sprintf("Source db[%s]", s.db.Name())
}

func (s *Section[E]) Close() error {
	return s.db.Close()
}
