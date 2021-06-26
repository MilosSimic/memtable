package main

import (
	"errors"
	sl "github.com/MilosSimic/skiplist"
)

const (
	TOMBSTONE_SIZE = 1
	TIMESTAMP_SIZE = 16
)

type Memtable struct {
	data *sl.SkipList
	size int
}

func New(maxHeight int, seed int64, size int) *Memtable {
	return &Memtable{
		data: sl.New(maxHeight, seed),
		size: size,
	}
}

func (m *Memtable) Add(key string, value []byte) (sl.Entry, error) {
	eSize := len(key) + len(value) + TOMBSTONE_SIZE + TIMESTAMP_SIZE
	if m.size >= m.size+eSize {
		m.size = m.size + eSize
		return m.data.Add(key, value), nil
	}
	return sl.Entry{}, errors.New("Memtable max size reached, flush the data")
}

func (m *Memtable) Delete(key string) (sl.Entry, error) {
	e, err := m.data.Get(key)
	if err != nil {
		return sl.Entry{}, err
	}
	eSize := len(e.Value) + len(key) + TOMBSTONE_SIZE + TIMESTAMP_SIZE
	e, err = m.data.TombstoneIt(key)
	if err != nil {
		return sl.Entry{}, err
	}
	m.size = m.size - eSize
	return e, nil
}

func (m *Memtable) Get(key string) (sl.Entry, error) {
	return m.data.Get(key)
}

func (m *Memtable) Serialize() map[string]sl.Entry {
	return m.data.ToMap()
}

func (m *Memtable) Update(key string, value []byte) (sl.Entry, error) {
	e, err := m.data.Get(key)
	if err != nil {
		return sl.Entry{}, err
	}
	//Get size before update
	currSize := len(e.Value) + len(key) + TOMBSTONE_SIZE + TIMESTAMP_SIZE
	//Get new size IF we update
	newSize := len(value) + len(key) + TOMBSTONE_SIZE + TIMESTAMP_SIZE
	sizediff := currSize - newSize
	//check can new size fits memtable
	if m.size >= m.size+sizediff {
		e, err := m.data.Update(key, value)
		if err != nil {
			return sl.Entry{}, nil
		}
		return e, nil
	}
	return sl.Entry{}, errors.New("There is no space in memtable.")
}

func main() {

}
