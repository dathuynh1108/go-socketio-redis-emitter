package emitter

import (
	"bytes"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

var (
	MsgPackEncoderPool = sync.Pool{
		New: func() any {
			encoder := msgpack.NewEncoder(nil)
			encoder.UseCompactInts(true)
			encoder.UseCompactFloats(true)
			return encoder
		},
	}
	MsgPackDecoderPool = sync.Pool{
		New: func() any {
			return msgpack.NewDecoder(nil)
		},
	}
)

func GetMsgPackEncoder() *msgpack.Encoder {
	return MsgPackEncoderPool.Get().(*msgpack.Encoder)
}

func PutMsgPackEncoder(encoder *msgpack.Encoder) {
	encoder.Reset(nil)
	MsgPackEncoderPool.Put(encoder)
}

func MsgPackMarshal(obj any, adaptJSON bool) ([]byte, error) {
	var (
		buf     = new(bytes.Buffer)
		encoder = GetMsgPackEncoder()
	)
	defer PutMsgPackEncoder(encoder)

	encoder.Reset(buf)
	if adaptJSON {
		encoder.SetCustomStructTag("json")
	}

	if err := encoder.Encode(obj); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
