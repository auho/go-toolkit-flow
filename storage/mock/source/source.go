package source

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Source[storage.MapEntry] = (*Mock[storage.MapEntry])(nil)

type mocker[E storage.Entry] interface {
	// id name, id, page size => stopId, items
	scan(string, *int64, int64) (*int64, []E)
	duplicate([]E) []E
}

type Mock[E storage.Entry] struct {
	storage.Storage
	id        int64
	total     int64 // 最大数量(总数)
	page      int64
	pageSize  int64
	totalPage int64
	amount    int64
	idName    string
	itemChan  chan []E
	mocker    mocker[E]
}

func newMock[E storage.Entry](config Config, mocker mocker[E]) *Mock[E] {
	m := &Mock[E]{}
	m.idName = config.IdName
	m.total = config.Total
	m.pageSize = config.PageSize
	m.mocker = mocker

	if m.total <= 0 {
		m.total = 1e2
	}

	if m.pageSize <= 0 {
		m.pageSize = 1e1
	}

	if m.idName == "" {
		m.idName = "id"
	}

	m.totalPage = int64(math.Ceil(float64(m.total) / float64(m.pageSize)))

	return m
}

func (m *Mock[E]) Scan() error {
	m.itemChan = make(chan []E)

	go func() {
		for i := int64(0); i < m.total; i += m.pageSize {
			size := m.pageSize
			if i+m.pageSize > m.total {
				size = m.total - i
			}

			_, items := m.mocker.scan(m.idName, &m.id, size)
			m.itemChan <- items

			atomic.AddInt64(&m.page, 1)
			atomic.AddInt64(&m.amount, int64(len(items)))
		}

		close(m.itemChan)
	}()

	return nil
}

func (m *Mock[E]) ReceiveChan() <-chan []E {
	return m.itemChan
}

func (m *Mock[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d, pageSize: %d", m.Title(), m.total, m.pageSize)}
}

func (m *Mock[E]) State() []string {
	return []string{fmt.Sprintf("amount: %d/%d, page: %d/%d(%d)", m.amount, m.total, m.page, m.totalPage, m.pageSize)}
}

func (m *Mock[E]) Copy(items []E) []E {
	return m.mocker.duplicate(items)
}

func (m *Mock[E]) Title() string {
	return "Source mock"
}

func (m *Mock[E]) Close() error {
	return nil
}
