package hash

import "testing"

func TestComputeHash(t *testing.T) {
	type args struct {
		arr []byte
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test string 1",
			args: args{
				arr: []byte("abcdef-123"),
			},
			want: 2675134171,
		},
		{
			name: "test string 2",
			args: args{
				arr: []byte("lorem ipsum dolor sit amet"),
			},
			want: 2828033737,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ComputeHash(tt.args.arr); got != tt.want {
				t.Errorf("ComputeHash() = %v, want %v", got, tt.want)
			}
		})
	}
}
