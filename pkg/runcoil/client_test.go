package runcoil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
)

func TestNewClientValidatesOptions(t *testing.T) {
	cases := []struct {
		name string
		opts []runcoil.ClientOption
		ok   bool
	}{
		{"valid https", []runcoil.ClientOption{runcoil.Endpoint("https://runcoil.internal:8080"), runcoil.Token("t")}, true},
		{"valid http", []runcoil.ClientOption{runcoil.Endpoint("http://localhost:7070"), runcoil.Token("t")}, true},
		{"no endpoint", []runcoil.ClientOption{runcoil.Token("t")}, false},
		{"no token", []runcoil.ClientOption{runcoil.Endpoint("http://localhost:7070")}, false},
		{"grpc scheme", []runcoil.ClientOption{runcoil.Endpoint("grpc://localhost:7070"), runcoil.Token("t")}, false},
		{"no host", []runcoil.ClientOption{runcoil.Endpoint("http://"), runcoil.Token("t")}, false},
		{"not a url", []runcoil.ClientOption{runcoil.Endpoint("://bad"), runcoil.Token("t")}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runcoil.NewClient(tc.opts...)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
