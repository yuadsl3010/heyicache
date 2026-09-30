// Package testmodel supplies cross-package generation fixtures.
// skill_name: development; plugin_name: development; skill_branch: main;
// skill_commit: 125eeb496930a08d171acf2dbc7f65f1aa3c4f74;
// skill_updated_at: 2026-09-30T15:53:35+08:00; skill_version_status: unknown
package testmodel

import "reflect"

type privateNumber int

type Child struct {
	Value         uint64
	Label         string
	unknownFields []byte
}

type Record struct {
	ID               uint64
	Name             string
	Data             []byte
	Inline           Child
	Children         []Child
	Pointers         []*Child
	Child            *Child
	Array            [2]Child
	Matrix           [2][2]Child
	Names            [2]string
	Numbers          [2]uint64
	ArrayPointers    [2]*Child
	ArraySlices      [2][]byte
	SkipInt          int    `heyicache:"skip"`
	SkipString       string `heyicache:"skip"`
	SkipSlice        []byte `heyicache:"skip"`
	SkipStruct       Child  `heyicache:"skip"`
	SkipArray        [2]int `heyicache:"skip"`
	XXX_unrecognized []byte
	Map              map[string]int
	ScalarPointer    *int
	Callback         func()
	Any              interface{}
	Channel          chan int
	unknownFields    []byte
	count            privateNumber
	flag             bool
	text             string
	pointer          *int
	mapping          map[string]int
	iface            interface{}
	fn               func()
	channel          chan int
	array            [2]int
	state            struct{ Data []byte }
}

func New() *Record {
	c := Child{Value: 7, Label: "nested", unknownFields: []byte{0xa0, 6, 1}}
	n := 42
	return &Record{
		ID: 17, Name: "original", Data: []byte("payload"), Inline: c,
		Children: []Child{c, c}, Pointers: []*Child{&c, nil}, Child: &c,
		Array: [2]Child{c, c}, Matrix: [2][2]Child{{c, c}, {c, c}},
		Names: [2]string{"one", "two"}, Numbers: [2]uint64{3, 4},
		ArrayPointers: [2]*Child{&c, nil}, ArraySlices: [2][]byte{[]byte("abc"), nil},
		SkipInt: 8, SkipString: "skip", SkipSlice: []byte{9}, SkipStruct: c, SkipArray: [2]int{1, 2},
		XXX_unrecognized: []byte{1}, Map: map[string]int{"x": 1}, ScalarPointer: &n,
		Callback: func() {}, Any: &n, Channel: make(chan int),
		unknownFields: []byte{0xa0, 6, 2}, count: 9, flag: true, text: "private",
		pointer: &n, mapping: map[string]int{"private": 1}, iface: &n, fn: func() {},
		channel: make(chan int), array: [2]int{1, 2}, state: struct{ Data []byte }{[]byte{7}},
	}
}

func (r *Record) PrivateZero() bool {
	return r.unknownFields == nil && r.count == 0 && !r.flag && r.text == "" &&
		r.pointer == nil && r.mapping == nil && r.iface == nil && r.fn == nil &&
		r.channel == nil && r.array == [2]int{} && r.state.Data == nil
}

func (r *Record) OmittedZero() bool {
	return r.PrivateZero() && r.SkipInt == 0 && r.SkipString == "" && r.SkipSlice == nil &&
		reflect.ValueOf(r.SkipStruct).IsZero() && r.SkipArray == [2]int{} &&
		r.XXX_unrecognized == nil && r.Map == nil && r.ScalarPointer == nil &&
		r.Callback == nil && r.Any == nil && r.Channel == nil
}

func (c *Child) PrivateZero() bool { return c == nil || c.unknownFields == nil }

// Same-package generation must have the same policy as cross-package generation.
type OnlyPrivate struct {
	value int
	data  []byte
	state struct{ P *int }
}

func NewOnlyPrivate() *OnlyPrivate { return &OnlyPrivate{value: 9, data: []byte{1}} }
func (v *OnlyPrivate) Zero() bool  { return reflect.ValueOf(*v).IsZero() }
