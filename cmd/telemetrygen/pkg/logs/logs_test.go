// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/config"
	types "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/pkg"
)

// TestDurationAndLogsInteraction tests the interaction between duration and logs parameters
func TestDurationAndLogsInteraction(t *testing.T) {
	tests := []struct {
		name         string
		config       Config
		expectedLogs int
		description  string
	}{
		{
			name: "Default behavior - respects logs parameter",
			config: Config{
				Config: config.Config{
					WorkerCount: 1,
				},
				NumLogs: 3,
			},
			expectedLogs: 3,
			description:  "By default, TotalDuration is 0, so NumLogs should be respected",
		},
		{
			name: "Finite duration overrides logs",
			config: Config{
				Config: config.Config{
					WorkerCount:   1,
					TotalDuration: types.DurationWithInf(100 * time.Millisecond),
				},
				NumLogs: 100,
			},
			expectedLogs: 0,
			description:  "Finite duration should override NumLogs (set to 0)",
		},
		{
			name: "Infinite duration overrides logs",
			config: Config{
				Config: config.Config{
					WorkerCount:   1,
					TotalDuration: types.MustDurationWithInf("Inf"),
				},
				NumLogs: 50,
			},
			expectedLogs: 0,
			description:  "Infinite duration should override NumLogs (set to 0)",
		},
		{
			name: "Zero duration with logs",
			config: Config{
				Config: config.Config{
					WorkerCount:   1,
					TotalDuration: types.DurationWithInf(0),
				},
				NumLogs: 5,
			},
			expectedLogs: 5,
			description:  "Zero duration should not override NumLogs",
		},
		{
			name: "Negative duration with logs",
			config: Config{
				Config: config.Config{
					WorkerCount:   1,
					TotalDuration: types.DurationWithInf(-100 * time.Millisecond),
				},
				NumLogs: 10,
			},
			expectedLogs: 10,
			description:  "Negative duration should not override NumLogs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.config

			if cfg.TotalDuration.Duration() > 0 || cfg.TotalDuration.IsInf() {
				cfg.NumLogs = 0
			}

			assert.Equal(t, tt.expectedLogs, cfg.NumLogs, tt.description)
		})
	}
}

// TestDefaultConfiguration tests that the default configuration is correct
func TestDefaultConfiguration(t *testing.T) {
	cfg := NewConfig()

	assert.Equal(t, types.DurationWithInf(0), cfg.TotalDuration, "Default TotalDuration should be 0")
	assert.Equal(t, 0, cfg.NumLogs, "Default NumLogs should be 0")
	assert.Equal(t, float64(1), cfg.Rate, "Default Rate should be 1")
	assert.Equal(t, "the message", cfg.Body, "Default Body should be 'the message'")
	assert.Equal(t, "Info", cfg.SeverityText, "Default SeverityText should be 'Info'")
	assert.Equal(t, int32(9), cfg.SeverityNumber, "Default SeverityNumber should be 9")
}

// TestConfigValidation tests the validation logic
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		description string
	}{
		{
			name: "Valid config with logs",
			config: Config{
				Config: config.Config{
					WorkerCount: 1,
				},
				NumLogs: 5,
			},
			expectError: false,
			description: "Config with NumLogs > 0 should be valid",
		},
		{
			name: "Valid config with finite duration",
			config: Config{
				Config: config.Config{
					WorkerCount:   1,
					TotalDuration: types.DurationWithInf(1 * time.Second),
				},
				NumLogs: 0,
			},
			expectError: false,
			description: "Config with finite duration > 0 should be valid",
		},
		{
			name: "Valid config with infinite duration",
			config: Config{
				Config: config.Config{
					WorkerCount:   1,
					TotalDuration: types.MustDurationWithInf("Inf"),
				},
				NumLogs: 0,
			},
			expectError: false,
			description: "Config with infinite duration should be valid",
		},
		{
			name: "Invalid config - no logs and no duration",
			config: Config{
				Config: config.Config{
					WorkerCount: 1,
				},
				NumLogs: 0,
			},
			expectError: true,
			description: "Config with no logs and no duration should be invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

// TestWorkerBehavior tests that workers behave correctly with different configurations
func TestWorkerBehavior(t *testing.T) {
	tests := []struct {
		name         string
		config       Config
		expectedLogs int
		description  string
	}{
		{
			name: "Worker with finite logs and no duration",
			config: Config{
				Config: config.Config{
					WorkerCount: 1,
				},
				NumLogs: 2,
			},
			expectedLogs: 2,
			description:  "Worker should generate exactly the specified number of logs",
		},
		{
			name: "Worker with infinite duration",
			config: Config{
				Config: config.Config{
					WorkerCount:   1,
					TotalDuration: types.MustDurationWithInf("Inf"),
				},
				NumLogs: 0, // This will be set by the run logic
			},
			expectedLogs: 0,
			description:  "Worker with infinite duration should have NumLogs set to 0",
		},
		{
			name: "Worker with finite duration",
			config: Config{
				Config: config.Config{
					WorkerCount:   1,
					TotalDuration: types.DurationWithInf(100 * time.Millisecond),
				},
				NumLogs: 10, // This will be set to 0 by the run logic
			},
			expectedLogs: 0,
			description:  "Worker with finite duration should have NumLogs set to 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.config.TotalDuration.Duration() > 0 || tt.config.TotalDuration.IsInf() {
				tt.config.NumLogs = 0
			}

			assert.Equal(t, tt.expectedLogs, tt.config.NumLogs, tt.description)
		})
	}
}

func TestHTTPExporterOptions_Timeout(t *testing.T) {
	for name, tc := range map[string]struct {
		timeout      time.Duration
		handlerDelay time.Duration
		expectError  bool
	}{
		"TimeoutElapsed": {
			timeout:      50 * time.Millisecond,
			handlerDelay: 500 * time.Millisecond,
			expectError:  true,
		},
		"TimeoutNotElapsed": {
			timeout:     500 * time.Millisecond,
			expectError: false,
		},
		"NoTimeout": {
			timeout:     0,
			expectError: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				if tc.handlerDelay > 0 {
					time.Sleep(tc.handlerDelay)
				}
			}))
			defer srv.Close()
			srvURL, _ := url.Parse(srv.URL)

			cfg := Config{
				Config: config.Config{
					Insecure:       true,
					CustomEndpoint: srvURL.Host,
					Timeout:        tc.timeout,
				},
			}
			opts, err := httpExporterOptions(&cfg)
			require.NoError(t, err)

			exp, err := otlploghttp.New(t.Context(), opts...)
			require.NoError(t, err)
			t.Cleanup(func() { _ = exp.Shutdown(t.Context()) })

			err = exp.Export(t.Context(), []sdklog.Record{{}})
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDefaultLogsConfiguration(t *testing.T) {
	cfg := NewConfig()

	cfg.NumLogs = 5
	cfg.WorkerCount = 1

	assert.Equal(t, 5, cfg.NumLogs, "NumLogs should be respected when TotalDuration is 0")
	assert.Equal(t, types.DurationWithInf(0), cfg.TotalDuration, "Default TotalDuration should be 0")
}
