// Copyright 2017 HootSuite Media Inc.
// SPDX-License-Identifier: Apache-2.0
// Modified hereafter by contributors to runatlantis/atlantis.
//
// Tests for the host-less legacy db.Database lookups, which must fail closed
// rather than guess when a repo name + pull number is shared across VCS hosts
// (design §419).

package etcd_test

import (
	"context"
	"testing"

	"github.com/runatlantis/atlantis/server/events/models"
	. "github.com/runatlantis/atlantis/testing"
)

// TestLegacyLookup_AmbiguousHostFailsClosed proves the host-less GetLock and
// UnlockByPull refuse to act when locks for the same repo/pull exist on more
// than one VCS host, and still resolve unambiguously for a single host.
func TestLegacyLookup_AmbiguousHostFailsClosed(t *testing.T) {
	d := newDB(t)

	a := projectLock("github.com", "o/r", ".", "default", 1)
	b := projectLock("gitlab.com", "o/r", ".", "default", 1)
	for _, l := range []models.ProjectLock{a, b} {
		ok, _, err := d.TryLock(l)
		Ok(t, err)
		Assert(t, ok, "setup lock should acquire")
	}

	// Host-less GetLock cannot disambiguate two hosts -> fail closed.
	_, err := d.GetLock(a.Project, a.Workspace)
	Assert(t, err != nil, "host-less GetLock must fail closed on multi-host ambiguity")

	// Host-less UnlockByPull likewise refuses rather than deleting the wrong host.
	_, err = d.UnlockByPull("o/r", 1)
	Assert(t, err != nil, "host-less UnlockByPull must fail closed on multi-host ambiguity")

	// The ambiguity must not have deleted either lock.
	scoped := d.Scoped()
	locks, err := scoped.ListProjectLocks(context.Background())
	Ok(t, err)
	Equals(t, 2, len(locks))
}

// TestLegacyLookup_SingleHostResolves proves the host-less lookups still work
// when only one host is present.
func TestLegacyLookup_SingleHostResolves(t *testing.T) {
	d := newDB(t)
	a := projectLock("github.com", "o/r", ".", "default", 1)
	ok, _, err := d.TryLock(a)
	Ok(t, err)
	Assert(t, ok, "setup lock should acquire")

	got, err := d.GetLock(a.Project, a.Workspace)
	Ok(t, err)
	Assert(t, got != nil, "single-host GetLock should resolve")

	removed, err := d.UnlockByPull("o/r", 1)
	Ok(t, err)
	Equals(t, 1, len(removed))
}

// TestUnlockByPullForClose_HostExact proves the close-path unlock targets the
// exact VCS host and never touches an identically-named pull on another host.
func TestUnlockByPullForClose_HostExact(t *testing.T) {
	d := newDB(t)
	gh := projectLock("github.com", "o/r", ".", "default", 1)
	gl := projectLock("gitlab.com", "o/r", ".", "default", 1)
	for _, l := range []models.ProjectLock{gh, gl} {
		ok, _, err := d.TryLock(l)
		Ok(t, err)
		Assert(t, ok, "setup lock should acquire")
	}

	removed, err := d.UnlockByPullForClose("o/r", "github.com", 1)
	Ok(t, err)
	Equals(t, 1, len(removed))
	Equals(t, "github.com", removed[0].Pull.BaseRepo.VCSHost.Hostname)

	// gitlab's lock must survive.
	locks, err := d.Scoped().ListProjectLocks(context.Background())
	Ok(t, err)
	Equals(t, 1, len(locks))
	Equals(t, "gitlab.com", locks[0].Pull.BaseRepo.VCSHost.Hostname)
}

// TestUnlockByPullScope_ClearsCleaningMarker proves that after cleanup the pull
// accepts new locks again (the lease-backed cleaning marker is released), so a
// completed cleanup never wedges acquisition.
func TestUnlockByPullScope_ClearsCleaningMarker(t *testing.T) {
	d := newDB(t)
	ctx := context.Background()
	lock := projectLock("github.com", "o/r", ".", "default", 3)
	ok, _, err := d.TryLock(lock)
	Ok(t, err)
	Assert(t, ok, "setup lock should acquire")

	removed, err := d.UnlockByPull("o/r", 3)
	Ok(t, err)
	Equals(t, 1, len(removed))

	// A fresh lock for the same pull must acquire again — the cleaning marker is
	// gone, not left wedging the pull.
	ok, _, err = d.TryLock(lock)
	Ok(t, err)
	Assert(t, ok, "re-lock after cleanup should succeed")
	_ = ctx
}
