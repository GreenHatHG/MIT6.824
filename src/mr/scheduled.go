package mr

import (
	"time"
)

func (m *Master) timeoutScheduledTask(workerId string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if v, ok := m.TaskList.Load(workerId); ok {
				task := v.(Task)
				if task.State != Idle {
					if task.WorkerType == Map {
						m.MapTaskChannel <- task.Files[0]
					} else if task.WorkerType == Reduce {
						m.ReduceTaskChannel <- workerId
					}
					m.initTaskState(workerId)
				}
			}
		}
	}
}
