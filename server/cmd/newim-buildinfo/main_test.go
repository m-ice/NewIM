package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/m-ice/NewIM/server/buildinfo"
)

func TestOutputKeepsUnknownFieldsAbsent(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(&stdout, &stderr, nil, func() (buildinfo.Info, error) {
		return buildinfo.Info{ServerVersion: "0.1.0-dev", GoVersion: "go1.27.1"}, nil
	})
	var value map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if code != 0 || stderr.Len() != 0 || len(value) != 2 || value["serverVersion"] != "0.1.0-dev" || value["goVersion"] != "go1.27.1" {
		t.Fatalf("wrong JSON/output contract: %d %v %q", code, value, stderr.String())
	}
}

func TestOutputPreservesExplicitCleanState(t *testing.T) {
	var stdout, stderr bytes.Buffer
	clean := false
	code := run(&stdout, &stderr, nil, func() (buildinfo.Info, error) {
		return buildinfo.Info{Modified: &clean}, nil
	})
	if code != 0 || !bytes.Contains(stdout.Bytes(), []byte(`"sourceModified":false`)) {
		t.Fatal("explicit clean state was lost")
	}
}

func TestErrorsDoNotEchoInputs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(&stdout, &stderr, []string{"private-input"}, nil); code != 2 || stderr.String() != "NIM_BUILD_INVALID_ARGUMENT\n" || stdout.Len() != 0 {
		t.Fatal("invalid argument contract changed")
	}
	stderr.Reset()
	if code := run(&stdout, &stderr, nil, func() (buildinfo.Info, error) { return buildinfo.Info{}, errors.New("private-input") }); code != 2 || stderr.String() != "NIM_BUILD_INVALID_METADATA\n" || stdout.Len() != 0 {
		t.Fatal("metadata error contract changed")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("private-output-error") }

func TestOutputFailure(t *testing.T) {
	for _, args := range [][]string{nil, {"--help"}} {
		var stderr bytes.Buffer
		code := run(failingWriter{}, &stderr, args, func() (buildinfo.Info, error) { return buildinfo.Info{}, nil })
		if code != 1 || stderr.String() != "NIM_BUILD_OUTPUT_FAILED\n" {
			t.Fatal("output failure must not report success or leak the writer error")
		}
	}
}

func TestHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(&stdout, &stderr, []string{"--help"}, nil); code != 0 || !bytes.Contains(stdout.Bytes(), []byte("Usage:")) || stderr.Len() != 0 {
		t.Fatal("help must work without reading build metadata")
	}
}
