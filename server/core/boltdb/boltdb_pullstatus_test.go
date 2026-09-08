// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package boltdb_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	. "github.com/runatlantis/atlantis/testing"
)

// TestPullStatus_ListPullStatuses writes a couple of pull statuses via the
// public API and asserts ListPullStatuses returns them all.
func TestPullStatus_ListPullStatuses(t *testing.T) {
	b := newTestDB2(t)
	defer b.Close()

	newPull := func(num int) models.PullRequest {
		return models.PullRequest{
			Num:        num,
			HeadCommit: "sha",
			URL:        "url",
			HeadBranch: "head",
			BaseBranch: "base",
			Author:     "lkysow",
			State:      models.OpenPullState,
			BaseRepo: models.Repo{
				FullName:          "runatlantis/atlantis",
				Owner:             "runatlantis",
				Name:              "atlantis",
				CloneURL:          "clone-url",
				SanitizedCloneURL: "clone-url",
				VCSHost: models.VCSHost{
					Hostname: "github.com",
					Type:     models.Github,
				},
			},
		}
	}

	pull1 := newPull(1)
	pull2 := newPull(2)

	results := []command.ProjectResult{
		{
			Command:    command.Plan,
			RepoRelDir: ".",
			Workspace:  "default",
			ProjectCommandOutput: command.ProjectCommandOutput{
				Failure: "failure",
			},
		},
	}

	_, err := b.UpdatePullWithResults(pull1, results)
	Ok(t, err)
	_, err = b.UpdatePullWithResults(pull2, results)
	Ok(t, err)

	statuses, err := b.ListPullStatuses()
	Ok(t, err)
	Equals(t, 2, len(statuses))

	// The order is not guaranteed, so index by pull number.
	byNum := map[int]models.PullStatus{}
	for _, s := range statuses {
		byNum[s.Pull.Num] = s
	}

	got1, ok := byNum[1]
	Assert(t, ok, "expected pull status for pull 1")
	Equals(t, pull1, got1.Pull) // nolint: staticcheck
	Equals(t, 1, len(got1.Projects))

	got2, ok := byNum[2]
	Assert(t, ok, "expected pull status for pull 2")
	Equals(t, pull2, got2.Pull) // nolint: staticcheck
	Equals(t, 1, len(got2.Projects))
}

// TestPullStatus_ListPullStatusesEmpty returns no error and an empty slice when
// there are no stored pull statuses.
func TestPullStatus_ListPullStatusesEmpty(t *testing.T) {
	b := newTestDB2(t)
	defer b.Close()

	statuses, err := b.ListPullStatuses()
	Ok(t, err)
	Equals(t, 0, len(statuses))
}
