package block

import (
	"sync"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/queue"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

type Task struct {
	Physical      *block.PhysicalBlock
	StorageWriter storage.ILayoutWriter
	IndexKey      *common.InternalKey
	IndexWriter   IIndexWriter
}

var taskPool = sync.Pool{
	New: func() interface{} {
		return &Task{}
	},
}

func (t *Task) Execute() error {
	//1. write the physical data to the durable storage
	bh, err := t.StorageWriter.WritePhysicalBlock(*t.Physical)
	if err != nil {
		return err
	}
	//2. write new index block (includes compute index KV, flush, ....)
	err = t.IndexWriter.Add(t.IndexKey, &bh)
	return err
}

func (t *Task) Release() {
	t.Physical = &block.PhysicalBlock{}
	t.StorageWriter = nil
	taskPool.Put(t)
}

func (t *Task) OnHold() {
	// do nothing
}

func SpawnNewTask() *Task {
	return taskPool.Get().(*Task)
}

var _ queue.ITask = (*Task)(nil)
