package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engines *engine_util.Engines
	config  *config.Config
	logger  *log.Logger
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	logger := log.New()
	logger.SetLevel(log.StringToLogLevel(conf.LogLevel))
	return &StandAloneStorage{config: conf, logger: logger}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.logger.Infof("-----Standalone Database Starting-----")
	s.engines = engine_util.NewEngines(engine_util.CreateDB(s.config.DBPath, false), nil, s.config.StoreAddr, "")
	s.logger.Infof("-----Standalone Database Running -----")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engines.Kv.Close()
	if err != nil {
		s.logger.Infof("Stop failed && error =%+v", err)
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandaloneReader{txn: s.engines.Kv.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			wb.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			wb.DeleteCF(m.Cf(), m.Key())
		}
	}
	return s.engines.WriteKV(wb)
}
