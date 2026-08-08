// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
	"github.com/runatlantis/atlantis/server/core/ownership"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

const testOwnerURL = "http://atlantis-0.atlantis-headless:4141"

type ownerClaimResult struct {
	record ownership.Record
	err    error
}

func TestOwnerStore_ConcurrentClaimKeepsOneLiveOwner(t *testing.T) {
	mr := miniredis.RunT(t)
	storeA := newTestOwnerStore(t, mr, "atlantis-0", testOwnerURL)
	storeB := newTestOwnerStore(t, mr, "atlantis-1", "http://atlantis-1.atlantis-headless:4141")
	key := testOwnershipKey(42)

	start := make(chan struct{})
	results := make(chan ownerClaimResult, 2)
	for _, store := range []*OwnerStore{storeA, storeB} {
		go func() {
			<-start
			record, err := store.Claim(context.Background(), key)
			results <- ownerClaimResult{record: record, err: err}
		}()
	}
	close(start)

	first := <-results
	second := <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, first.record, second.record)
	require.NotEmpty(t, first.record.ClaimID)
	require.NotEmpty(t, first.record.InstanceID)
	require.NotEqual(t, storeA.Owns(key, first.record.ClaimID), storeB.Owns(key, first.record.ClaimID))
}

func TestOwnerStore_ExpiredOwnerCanBeReclaimed(t *testing.T) {
	mr := miniredis.RunT(t)
	storeA := newTestOwnerStoreWithTTL(t, mr, "atlantis-0", testOwnerURL, 30*time.Second)
	storeB := newTestOwnerStoreWithTTL(t, mr, "atlantis-1", "http://atlantis-1.atlantis-headless:4141", 30*time.Second)
	key := testOwnershipKey(43)

	_, err := storeA.Claim(context.Background(), key)
	require.NoError(t, err)
	mr.FastForward(31 * time.Second)

	reclaimed, err := storeB.Claim(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, "atlantis-1", reclaimed.ReplicaID)
	require.True(t, storeB.Owns(key, reclaimed.ClaimID))
}

func TestOwnerStore_RenewExtendsTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	store := newTestOwnerStoreWithTTL(t, mr, "atlantis-0", testOwnerURL, 30*time.Second)
	key := testOwnershipKey(44)

	claim, err := store.Claim(context.Background(), key)
	require.NoError(t, err)
	mr.FastForward(20 * time.Second)
	renewed, err := store.renewClaim(context.Background(), key, claim)
	require.NoError(t, err)
	require.True(t, renewed)
	mr.FastForward(20 * time.Second)

	current, found, err := store.Current(context.Background(), key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, claim, current)
}

func TestOwnerStore_ReusedReplicaIDDoesNotOwnPriorProcessClaim(t *testing.T) {
	mr := miniredis.RunT(t)
	oldProcess := newTestOwnerStore(t, mr, "atlantis-0", testOwnerURL)
	newProcess := newTestOwnerStore(t, mr, "atlantis-0", testOwnerURL)
	key := testOwnershipKey(45)

	oldClaim, err := oldProcess.Claim(context.Background(), key)
	require.NoError(t, err)
	observed, err := newProcess.Claim(context.Background(), key)
	require.NoError(t, err)

	require.Equal(t, oldClaim, observed)
	require.NotEqual(t, oldProcess.instanceID, newProcess.instanceID)
	require.False(t, newProcess.Owns(key, observed.ClaimID))
}

func TestOwnerStore_StaleClaimCannotRenewOrReleaseReplacement(t *testing.T) {
	mr := miniredis.RunT(t)
	storeA := newTestOwnerStoreWithTTL(t, mr, "atlantis-0", testOwnerURL, 30*time.Second)
	storeB := newTestOwnerStoreWithTTL(t, mr, "atlantis-1", "http://atlantis-1.atlantis-headless:4141", 30*time.Second)
	key := testOwnershipKey(46)

	claimA, err := storeA.Claim(context.Background(), key)
	require.NoError(t, err)
	mr.FastForward(31 * time.Second)
	claimB, err := storeB.Claim(context.Background(), key)
	require.NoError(t, err)

	renewed, err := storeA.renewClaim(context.Background(), key, claimA)
	require.NoError(t, err)
	require.False(t, renewed)
	require.NoError(t, storeA.Release(context.Background(), key, claimA.ClaimID))

	current, found, err := storeB.Current(context.Background(), key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, claimB, current)
}

func TestOwnerStore_BeginDrainRejectsNewClaimsWithoutReleasingLiveClaims(t *testing.T) {
	mr := miniredis.RunT(t)
	store := newTestOwnerStore(t, mr, "atlantis-0", testOwnerURL)
	ownedKey := testOwnershipKey(47)
	newKey := testOwnershipKey(48)
	claim, err := store.Claim(context.Background(), ownedKey)
	require.NoError(t, err)

	store.BeginDrain()
	_, err = store.Claim(context.Background(), newKey)
	require.ErrorIs(t, err, ownership.ErrDraining)
	require.ErrorIs(t, store.Ready(context.Background()), ownership.ErrDraining)
	current, found, err := store.Current(context.Background(), ownedKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, claim, current)
}

func TestOwnerStore_CloseReleasesOwnedClaims(t *testing.T) {
	mr := miniredis.RunT(t)
	store := newTestOwnerStore(t, mr, "atlantis-0", testOwnerURL)
	observer := newTestOwnerStore(t, mr, "atlantis-1", "http://atlantis-1.atlantis-headless:4141")
	key := testOwnershipKey(49)
	_, err := store.Claim(context.Background(), key)
	require.NoError(t, err)

	require.NoError(t, store.Close())
	_, found, err := observer.Current(context.Background(), key)
	require.NoError(t, err)
	require.False(t, found)
}

func TestOwnerStore_StoredRecordContainsOnlyRoutingMetadata(t *testing.T) {
	mr := miniredis.RunT(t)
	store := newTestOwnerStore(t, mr, "atlantis-0", testOwnerURL)
	key := testOwnershipKey(50)
	_, err := store.Claim(context.Background(), key)
	require.NoError(t, err)

	redisKey, err := redisOwnershipKey(key)
	require.NoError(t, err)
	serialized, err := mr.Get(redisKey)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal([]byte(serialized), &fields))
	require.ElementsMatch(t, []string{
		"schema_version", "replica_id", "instance_id", "advertise_url", "claim_id", "claimed_at",
	}, mapKeys(fields))
	require.NotContains(t, serialized, "token")
	require.NotContains(t, serialized, "password")
	require.NotContains(t, serialized, "command")
}

func TestOwnerStore_ReadyFailsAfterPersistentRenewalErrors(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{
		Addr:         mr.Addr(),
		DialTimeout:  10 * time.Millisecond,
		ReadTimeout:  10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
		MaxRetries:   0,
	})
	store, err := NewOwnerStoreWithClient(client, OwnerStoreConfig{
		ReplicaID: "atlantis-0", AdvertiseURL: testOwnerURL, TTL: 60 * time.Millisecond,
	}, logging.NewNoopLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close()
		_ = client.Close()
	})
	_, err = store.Claim(context.Background(), testOwnershipKey(51))
	require.NoError(t, err)
	mr.Close()

	require.Eventually(t, func() bool {
		return store.Ready(context.Background()) != nil
	}, 2*time.Second, 10*time.Millisecond)
}

func newTestOwnerStore(t *testing.T, mr *miniredis.Miniredis, replicaID, advertiseURL string) *OwnerStore {
	return newTestOwnerStoreWithTTL(t, mr, replicaID, advertiseURL, 30*time.Second)
}

func newTestOwnerStoreWithTTL(
	t *testing.T,
	mr *miniredis.Miniredis,
	replicaID string,
	advertiseURL string,
	ttl time.Duration,
) *OwnerStore {
	t.Helper()
	client := redislib.NewClient(&redislib.Options{Addr: mr.Addr()})
	store, err := NewOwnerStoreWithClient(client, OwnerStoreConfig{
		ReplicaID: replicaID, AdvertiseURL: advertiseURL, TTL: ttl,
	}, logging.NewNoopLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close()
		_ = client.Close()
	})
	return store
}

func testOwnershipKey(pullNum int) ownership.Key {
	return ownership.Key{VCSHostname: "github.com", RepoFullName: "owner/repo", PullNum: pullNum}
}

func mapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
