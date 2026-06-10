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
	Query(se *Section[E], startId, endId int64) ([]E, error)
	Copy([]E) []E
}

// Section 分段查询
type Section[E storage.Entry] struct {
	storage.Storage
	db     *database.DB
	scanSw sync.WaitGroup
	conf   *QueryConfig

	total     int64
	totalPage int64
	startId   int64 // 闭区间
	endId     int64 // 闭区间

	failureLastId []int
	idRangeChan   chan []int64 // []in64{left id, right id} 包含两端
	rowsChan      chan []E
	state         *storage.PageState
	sq            sectionQuery[E]
}

func newSection[E storage.Entry](config *QueryConfig, sq sectionQuery[E], b database.BuildDb) (*Section[E], error) {
	s := &Section[E]{}
	err := s.config(config, b)
	if err != nil {
		return nil, err
	}

	s.sq = sq

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
		s.conf.PageSize,
		s.startId,
		s.endId)}
}

func (s *Section[E]) Copy(items []E) []E {
	return s.sq.Copy(items)
}

func (s *Section[E]) Scan() error {
	s.state.StatusScan()
	s.state.DurationStart()
	s.idRangeChan = make(chan []int64, s.conf.Concurrency)
	s.rowsChan = make(chan []E, s.conf.Concurrency)

	err := s.idRange()
	if err != nil {
		return err
	}

	go s.idSection()
	s.scanRows()

	go func() {
		s.scanSw.Wait()
		close(s.rowsChan)

		s.state.DurationStop()
		s.state.StatusFinish()
	}()

	return nil
}

func (s *Section[E]) ReceiveChan() <-chan []E {
	return s.rowsChan
}

// 根据 start id， end id，分段（left id， right id）并分发 id section
func (s *Section[E]) idSection() {
	_break := false
	_startId := s.startId
	_rightId := int64(0)
	for {
		_rightId = _startId + s.conf.PageSize - 1
		if _rightId >= s.endId {
			_rightId = s.endId
			_break = true
		}

		s.idRangeChan <- []int64{_startId, _rightId}

		if _break {
			break
		}

		_startId += s.conf.PageSize
	}

	close(s.idRangeChan)
}

// 根据 id section（left id，right id） 查询 rows
func (s *Section[E]) scanRows() {
	for i := 0; i < s.conf.Concurrency; i++ {
		s.scanSw.Add(1)
		go func() {
			for idRange := range s.idRangeChan {
				atomic.AddInt64(&s.state.Page, 1)

				leftId := idRange[0]
				rightId := idRange[1]

				rows, err := s.sq.Query(s, leftId, rightId)
				if err != nil {
					s.LogFatalWithTitle(fmt.Sprintf("left id[%d] - right id[%d]", leftId, rightId), err)
				}

				if len(rows) == 0 {
					continue
				}

				s.state.AddAmount(int64(len(rows)))

				s.rowsChan <- rows
			}

			s.scanSw.Done()
		}()
	}
}

func (s *Section[E]) config(config *QueryConfig, b database.BuildDb) (err error) {
	s.conf = config

	s.total = config.Maximum
	s.startId = config.StartId
	s.endId = config.EndId

	s.db, err = b()
	if err != nil {
		return
	}

	err = s.db.Ping()
	if err != nil {
		return
	}

	if s.conf.Concurrency <= 0 {
		s.conf.Concurrency = runtime.NumCPU()
	}

	if s.conf.PageSize <= 0 {
		err = fmt.Errorf("page size[%d] is error", s.conf.PageSize)
		return
	}

	if s.total > 0 && s.conf.PageSize > s.total {
		s.conf.PageSize = s.total
	}

	s.state = storage.NewPageState()
	s.state.Concurrency = s.conf.Concurrency
	s.state.Title = s.Title()
	s.state.StatusConfig()

	return
}

// id range
func (s *Section[E]) idRange() error {
	var row struct {
		Max int64
		Min int64
	}

	query := fmt.Sprintf("MAX(%s) AS max, MIN(%s) AS min", s.conf.IdName, s.conf.IdName)
	err := s.db.Table(s.conf.TableName).Select(query).Scan(&row).Error
	if err != nil {
		return fmt.Errorf("id range %w", err)
	}

	if row.Min > s.startId {
		s.startId = row.Min
	}

	if s.endId <= 0 || s.endId > row.Max {
		s.endId = row.Max
	}

	if s.endId < s.startId {
		return fmt.Errorf("mysql max id %d < start id %d", s.endId, s.startId)
	}

	total := s.endId - s.startId + 1
	if s.total == 0 {
		s.total = total
	} else {
		if s.total < total {
			s.endId = s.startId + s.total - 1
		} else if s.total > total {
			s.total = total
		}
	}

	if s.conf.PageSize > s.total {
		s.conf.PageSize = s.total
	}

	s.totalPage = int64(math.Ceil(float64(s.total) / float64(s.conf.PageSize)))

	s.state.PageSize = s.conf.PageSize
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
