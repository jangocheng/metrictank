package chunk

import (
	"errors"

	"github.com/grafana/metrictank/mdata/chunk/tsz"
)

var (
	errUnknownChunkFormat = errors.New("unrecognized chunk format in cassandra")
	errUnknownSpanCode    = errors.New("corrupt data, chunk span code is not known")
	errShort              = errors.New("chunk is too short")
)

//go:generate msgp
type IterGen struct {
	T0 uint32
	B  []byte
}

func NewGen(t0 uint32, b []byte) (*IterGen, error) {
	switch Format(b[0]) {
	case FormatStandardGoTsz:
		if len(b) == 1 {
			return nil, errShort
		}
	case FormatStandardGoTszWithSpan:
		if len(b) <= 2 {
			return nil, errShort
		}
		if int(b[1]) >= len(ChunkSpans) {
			return nil, errUnknownSpanCode
		}
	case FormatGoTszLongWithSpan:
		if len(b) <= 2 {
			return nil, errShort
		}
		if int(b[1]) >= len(ChunkSpans) {
			return nil, errUnknownSpanCode
		}
	default:
		return nil, errUnknownChunkFormat
	}

	return &IterGen{
		t0,
		b,
	}, nil
}

func NewBareIterGen(t0 uint32, b []byte) *IterGen {
	return &IterGen{t0, b}
}

func (ig IterGen) Format() Format {
	return Format(ig.B[0])
}

// TODO we used to
// b := make([]byte, len(ig.B), len(ig.B))
// copy(b, ig.B)
// needed?
func (ig *IterGen) Get() (Iter, error) {
	switch ig.Format() {
	case FormatStandardGoTsz:
		return tsz.NewIterator4h(ig.B[1:])
	case FormatStandardGoTszWithSpan:
		return tsz.NewIterator4h(ig.B[2:])
	}
	// FormatGoTszLongWithSpan:
	return tsz.NewIteratorLong(ig.T0, ig.B[2:])
}

func (ig *IterGen) Span() uint32 {
	if Format(ig.B[0]) == FormatStandardGoTsz {
		return 0 // we don't know what the span is. sorry.
	}
	// already validated at IterGen creation time
	return ChunkSpans[SpanCode(ig.B[1])]
}

func (ig *IterGen) Size() uint64 { // TODO this is different than before. problem?
	return uint64(len(ig.B))
}

func (ig IterGen) Bytes() []byte { // TODO this is different than before. problem?
	return ig.B
}

// end of itergen (exclusive). next t0
func (ig IterGen) EndTs() uint32 {
	return ig.T0 + ig.Span()
}

//msgp:ignore IterGensAsc
type IterGensAsc []IterGen

func (a IterGensAsc) Len() int           { return len(a) }
func (a IterGensAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a IterGensAsc) Less(i, j int) bool { return a[i].T0 < a[j].T0 }
