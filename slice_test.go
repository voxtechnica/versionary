package versionary

import (
	"reflect"
	"testing"
)

func TestBatch(t *testing.T) {
	type args struct {
		items     []int
		batchSize int
	}
	tests := []struct {
		name string
		args args
		want [][]int
	}{
		{
			name: "empty slice",
			args: args{
				items:     []int{},
				batchSize: 2,
			},
			want: [][]int{},
		},
		{
			name: "single batch",
			args: args{
				items:     []int{1, 2, 3},
				batchSize: 3,
			},
			want: [][]int{{1, 2, 3}},
		},
		{
			name: "multiple batches",
			args: args{
				items:     []int{1, 2, 3, 4, 5},
				batchSize: 2,
			},
			want: [][]int{{1, 2}, {3, 4}, {5}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Batch(tt.args.items, tt.args.batchSize); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Batch() = %v, want %v", got, tt.want)
			}
		})
	}
}
