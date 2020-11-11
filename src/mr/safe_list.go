package mr

import (
	"container/list"
	"sync"
)

type SafeList struct {
	MU   *sync.Mutex
	Data *list.List
}

func NewSafeList() *SafeList {
	return &SafeList{
		MU:   new(sync.Mutex),
		Data: list.New(),
	}
}

func (s *SafeList) Add(ele interface{}) {
	s.MU.Lock()
	s.Data.PushBack(ele)
	s.MU.Unlock()
}

func (s *SafeList) Remove(parameters ...interface{}) {
	s.MU.Lock()
	for e := s.Data.Front(); e != nil; e = e.Next() {
		if len(parameters) == 1 {
			val := e.Value.(string)
			if val == parameters[0] {
				s.Data.Remove(e)
			}
			break
		} else if len(parameters) == 2 {
			task := e.Value.(Task)
			if task.File == parameters[0] && task.Type == parameters[1] {
				s.Data.Remove(e)
				break
			}
		}
	}
}

func (s *SafeList) Len() int {
	s.MU.Lock()
	l := s.Data.Len()
	s.MU.Unlock()
	return l
}

func (s *SafeList) Iterator() {
	s.MU.Lock()

	s.MU.Unlock()
}
