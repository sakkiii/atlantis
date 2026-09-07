// Copyright 2017 HootSuite Media Inc.
// SPDX-License-Identifier: Apache-2.0
// Modified hereafter by contributors to runatlantis/atlantis.
//
// This file implements the versioned JSON value envelope (design §619). Values
// use versioned JSON envelopes; decoding is strict for a recognized version. A
// binary must explicitly implement every older and newer record version it
// claims to read, so an unrecognized version is a hard error rather than a
// best-effort parse.
package etcd

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// envelope wraps every stored value with its record kind and version. The kind
// guards against a key being decoded as the wrong record type; the version
// selects the decoder.
type envelope struct {
	Kind    string          `json:"kind"`
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload"`
}

// encodeValue serializes payload into a versioned envelope. kind and version
// identify the record so a reader can refuse an unexpected type or version.
func encodeValue(kind string, version int, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling %s payload: %w", kind, err)
	}
	return json.Marshal(envelope{Kind: kind, Version: version, Payload: raw})
}

// decodeValue strictly decodes a versioned envelope. It fails closed on a kind
// mismatch or a version outside [minVersion, maxVersion]; absence of a known
// version is never treated as an empty record.
func decodeValue(data []byte, kind string, minVersion, maxVersion int, payload any) error {
	var env envelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return fmt.Errorf("decoding %s envelope: %w", kind, err)
	}
	if env.Kind != kind {
		return fmt.Errorf("expected record kind %q but stored value is %q", kind, env.Kind)
	}
	if env.Version < minVersion || env.Version > maxVersion {
		return fmt.Errorf("%s record version %d is outside readable range [%d,%d]; this binary does not implement it", kind, env.Version, minVersion, maxVersion)
	}
	if err := json.Unmarshal(env.Payload, payload); err != nil {
		return fmt.Errorf("decoding %s payload v%d: %w", kind, env.Version, err)
	}
	return nil
}
