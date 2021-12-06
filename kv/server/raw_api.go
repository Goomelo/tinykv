package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	rawGetResponse := new(kvrpcpb.RawGetResponse)
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	val, err := reader.GetCF(req.GetCf(), req.Key)
	if err != nil {
		rawGetResponse.Error = err.Error()
		rawGetResponse.NotFound = true
		return rawGetResponse, nil
	}
	if len(val) == 0 {
		rawGetResponse.NotFound = true
	}
	rawGetResponse.Value = val
	return rawGetResponse, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	rawPutResponse := new(kvrpcpb.RawPutResponse)
	modify := []storage.Modify{
		{Data: storage.Put{Value: req.Value, Key: req.Key, Cf: req.Cf}},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		rawPutResponse.Error = err.Error()
	}

	return rawPutResponse, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	rawDeleteResponse := new(kvrpcpb.RawDeleteResponse)
	modify := []storage.Modify{
		{
			Data: storage.Delete{Cf: req.Cf, Key: req.Key},
		},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		rawDeleteResponse.Error = err.Error()
	}
	return rawDeleteResponse, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	rawScanResponse := new(kvrpcpb.RawScanResponse)
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	for i := uint32(0); iter.Valid() && i < req.Limit; i += 1 {
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()
		rawScanResponse.Kvs = append(rawScanResponse.Kvs, &kvrpcpb.KvPair{Key: key, Value: val})
		iter.Next()
	}
	return rawScanResponse, nil

}
