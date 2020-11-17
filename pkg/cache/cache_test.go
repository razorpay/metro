package cache

import "testing"

func TestNewCache(t *testing.T) {
	type args struct {
		config *Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "In-memory test",
			args: args{
				config: &Config{
					Driver: "memory",
					InMemory: InMemoryConfig{
						defaultExpiration: 15,
						cleanupInterval:   30,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "In-memory test",
			args: args{
				config: &Config{
					Driver: "nil",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewCache(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("NewCache() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
