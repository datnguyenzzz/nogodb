package row_block

import (
	"sync"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/queue"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

type task struct {
	physical      *common.PhysicalBlock
	storageWriter storage.IWriter
	indexKey      *common.InternalKey
	indexWriter   *indexWriter
}

var taskPool = sync.Pool{
	New: func() interface{} {
		return &task{}
	},
}

func (t *task) Execute() error {
	//1. write the physical data to the durable storage
	bh, err := t.storageWriter.WritePhysicalBlock(*t.physical)
	if err != nil {
		return err
	}
	//2. write new index block (includes compute index KV, flush, ....)
	err = t.indexWriter.add(t.indexKey, &bh)
	return err
}

func (t *task) Release() {
	t.physical = &common.PhysicalBlock{}
	t.storageWriter = nil
	taskPool.Put(t)
}

func (t *task) OnHold() {
	// do nothing
}

func spawnNewTask() *task {
	return taskPool.Get().(*task)
}

var _ queue.ITask = (*task)(nil)
