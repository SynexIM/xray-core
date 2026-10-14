package pipe_test

import (
	"testing"

	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/transport/pipe"
)

func TestOverflowCountsPacketsAndKeepsAcceptedData(t *testing.T) {
	var dropped int64
	reader, writer := New(WithSizeLimit(1), DiscardOverflow(), OnOverflow(func(count int64) { dropped += count }))
	accepted := buf.New()
	accepted.WriteString("accepted")
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{accepted}); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("a")), buf.FromBytes([]byte("b"))}); err != nil {
		t.Fatal(err)
	}
	if dropped != 2 {
		t.Fatalf("应按包计数，得到 %d", dropped)
	}
	data, err := reader.ReadMultiBuffer()
	if err != nil || data.String() != "accepted" {
		t.Fatalf("丢包改变了已接受数据: %v %v", data, err)
	}
	buf.ReleaseMulti(data)
}
