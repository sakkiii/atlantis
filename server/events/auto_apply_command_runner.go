// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/locking"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/runatlantis/atlantis/server/recovery"
	tally "github.com/uber-go/tally/v4"
)

func NewAutoApplyCommandRunner(
	vcsClient vcs.Client,
	applyCommandLocker locking.ApplyLockChecker,
	commitStatusUpdater CommitStatusUpdater,
	prjCommandBuilder ProjectApplyCommandBuilder,
	prjCmdRunner ProjectApplyCommandRunner,
	cancellationTracker CancellationTracker,
	autoMerger *AutoMerger,
	pullUpdater *PullUpdater,
	dbUpdater *DBUpdater,
	database db.Database,
	parallelPoolSize int,
	SilenceNoProjects bool,
	silenceVCSStatusNoProjects bool,
	workingDirLocker WorkingDirLocker,
	pullReqStatusFetcher vcs.PullReqStatusFetcher,
	livePullHeadFetcher LivePullHeadFetcher,
	workingDir WorkingDir,
	disableAutomergeLabel string,
	logger logging.SimpleLogging,
	scope tally.Scope,
	globalCfg valid.GlobalCfg,
	requirementHandler *DefaultCommandRequirementHandler,
	teamAllowlistChecker command.TeamAllowlistChecker,
	allowForkPRs bool,
	allowForkPRsFlag string,
) *defaultAutoApplyCommandRunner {
	return &defaultAutoApplyCommandRunner{
		vcsClient:                  vcsClient,
		DisableApplyAll:            false, // auto-apply doesn't need this flag
		locker:                     applyCommandLocker,
		commitStatusUpdater:        commitStatusUpdater,
		prjCmdBuilder:              prjCommandBuilder,
		prjCmdRunner:               prjCmdRunner,
		cancellationTracker:        cancellationTracker,
		autoMerger:                 autoMerger,
		pullUpdater:                pullUpdater,
		dbUpdater:                  dbUpdater,
		Database:                   database,
		parallelPoolSize:           parallelPoolSize,
		workingDirLocker:           workingDirLocker,
		pullReqStatusFetcher:       pullReqStatusFetcher,
		livePullHeadFetcher:        livePullHeadFetcher,
		workingDir:                 workingDir,
		disableAutomergeLabel:      disableAutomergeLabel,
		SilenceNoProjects:          SilenceNoProjects,
		silenceVCSStatusNoProjects: silenceVCSStatusNoProjects,
		Logger:                     logger,
		scope:                      scope,
		globalCfg:                  globalCfg,
		requirementHandler:         requirementHandler,
		teamAllowlistChecker:       teamAllowlistChecker,
		allowForkPRs:               allowForkPRs,
		allowForkPRsFlag:           allowForkPRsFlag,
	}
}

// approvalStatusFetchRetries is how many times the pull request approval status
// is re-fetched when the pull request is not yet reported as approved. GitHub
// can deliver a pull_request_review webhook before its aggregate approval state
// has been updated.
const approvalStatusFetchRetries = 2

// approvalStatusFetchRetryInterval is the delay between approval status
// re-fetches.
const approvalStatusFetchRetryInterval = time.Second

type defaultAutoApplyCommandRunner struct {
	DisableApplyAll            bool
	Database                   db.Database
	locker                     locking.ApplyLockChecker
	vcsClient                  vcs.Client
	commitStatusUpdater        CommitStatusUpdater
	prjCmdBuilder              ProjectApplyCommandBuilder
	prjCmdRunner               ProjectApplyCommandRunner
	cancellationTracker        CancellationTracker
	autoMerger                 *AutoMerger
	pullUpdater                *PullUpdater
	dbUpdater                  *DBUpdater
	parallelPoolSize           int
	workingDirLocker           WorkingDirLocker
	pullReqStatusFetcher       vcs.PullReqStatusFetcher
	livePullHeadFetcher        LivePullHeadFetcher
	workingDir                 WorkingDir
	disableAutomergeLabel      string
	SilenceNoProjects          bool
	silenceVCSStatusNoProjects bool
	Logger                     logging.SimpleLogging
	scope                      tally.Scope
	globalCfg                  valid.GlobalCfg
	requirementHandler         *DefaultCommandRequirementHandler
	teamAllowlistChecker       command.TeamAllowlistChecker
	allowForkPRs               bool
	allowForkPRsFlag           string
}

func (a *defaultAutoApplyCommandRunner) Run(baseRepo models.Repo, headRepo models.Repo, pull models.PullRequest, user models.User) {
	log := a.Logger.WithHistory(
		"repo", baseRepo.FullName,
		"pull", pull.Num,
	)
	defer a.logPanics(baseRepo, pull.Num, log)

	// Check if PR is open
	if pull.State != models.OpenPullState {
		log.Info("ignoring auto-apply because pull request is not open")
		return
	}

	// Check branch matching
	repo := a.globalCfg.MatchingRepo(baseRepo.ID())
	if repo != nil && !repo.BranchMatches(pull.BaseBranch) {
		log.Info("ignoring auto-apply because branch doesn't match")
		return
	}

	// Check fork PR
	if !a.allowForkPRs && headRepo.Owner != baseRepo.Owner {
		log.Info("ignoring auto-apply because pull request is from a fork and fork PRs are disabled")
		return
	}

	// Check global apply lock
	locked, err := a.IsLocked()
	if err != nil {
		log.Err("checking global apply lock: %s", err)
		if commentErr := a.vcsClient.CreateComment(log, baseRepo, pull.Num, "Failed to check global apply lock. Running `atlantis apply` is not allowed until the lock backend is reachable.", command.Apply.String()); commentErr != nil {
			log.Err("unable to comment on pull request: %s", commentErr)
		}
		return
	}
	if locked {
		log.Info("ignoring auto-apply because apply disabled globally")
		if err := a.vcsClient.CreateComment(log, baseRepo, pull.Num, "Running `atlantis apply` is disabled.", command.Apply.String()); err != nil {
			log.Err("unable to comment on pull request: %s", err)
		}
		return
	}

	// Check team allowlist
	if a.teamAllowlistChecker != nil && a.teamAllowlistChecker.HasRules() {
		// Fetch user teams first (needed for permission check)
		if err := a.fetchUserTeams(log, baseRepo, &user); err != nil {
			log.Err("unable to fetch user teams: %s", err)
			return
		}
		ok, err := a.checkUserPermissions(baseRepo, &user, command.Apply.String())
		if err != nil {
			log.Err("unable to check user permissions: %s", err)
			return
		}
		if !ok {
			log.Info("ignoring auto-apply because user does not have permissions")
			return
		}
	}

	// Lock the working directory
	var unlockPullApply func()
	if a.workingDirLocker != nil {
		unlockPullApply, err = a.workingDirLocker.TryLockPull(baseRepo.FullName, pull.Num, command.Apply, WorkingDirLockMetadataForPull(pull))
		if err != nil {
			log.Err("failed to lock working directory: %s", err)
			return
		}
		defer unlockPullApply()
	}

	// Fetch pull status
	status, err := a.Database.GetPullStatus(pull)
	if err != nil {
		log.Err("fetching current plan status: %s", err)
		return
	}

	// Fetch live pull identity
	livePull, err := a.refreshLivePullIdentity(log, pull)
	if err != nil {
		log.Err("fetching live pull request: %s", err)
		return
	}

	// Update pull with live identity
	if livePull.HeadCommit != "" {
		pull.HeadCommit = livePull.HeadCommit
		if livePull.BaseBranch != "" {
			pull.BaseBranch = livePull.BaseBranch
		}
	}

	// Fetch pull request status (approval, mergeability) using updated pull identity.
	// GitHub can deliver the pull_request_review webhook before its aggregate
	// approval state has been updated, so the first fetch may still report the
	// pull request as not approved. Retry briefly in that case.
	pullReqStatus, err := a.pullReqStatusFetcher.FetchPullStatus(log, pull)
	if err != nil {
		log.Warn("unable to get pull request status: %s. Continuing with mergeable and approved assumed false", err)
	}
	for i := 0; i < approvalStatusFetchRetries && !pullReqStatus.ApprovalStatus.IsApproved; i++ {
		time.Sleep(approvalStatusFetchRetryInterval)
		pullReqStatus, err = a.pullReqStatusFetcher.FetchPullStatus(log, pull)
		if err != nil {
			log.Warn("unable to get pull request status: %s. Continuing with mergeable and approved assumed false", err)
		}
	}

	// Build apply commands for all projects
	cmd := &CommentCommand{
		Name: command.Apply,
	}
	ctx := &command.Context{
		User:                 user,
		Log:                  log,
		Scope:                a.scope.SubScope("auto_apply"),
		Pull:                 pull,
		HeadRepo:             headRepo,
		PullStatus:           status,
		PullRequestStatus:    pullReqStatus,
		Trigger:              command.AutoTrigger,
		TeamAllowlistChecker: a.teamAllowlistChecker,
	}

	projectCmds, err := a.prjCmdBuilder.BuildApplyCommands(ctx, cmd)
	if err != nil {
		log.Err("building apply commands: %s", err)
		return
	}

	// Filter to only projects with auto_apply: true
	var autoApplyCmds []command.ProjectContext
	for _, pc := range projectCmds {
		if pc.AutoApply {
			autoApplyCmds = append(autoApplyCmds, pc)
		}
	}

	if len(autoApplyCmds) == 0 {
		log.Info("no projects with auto_apply: true found")
		return
	}

	// Validate apply requirements for each project
	validCmds := a.validateAndFilterProjects(log, autoApplyCmds)
	if len(validCmds) == 0 {
		log.Info("no projects passed apply requirements")
		return
	}

	// Update pending status
	if len(validCmds) > 0 && !a.silenceVCSStatusNoProjects {
		if err := a.commitStatusUpdater.UpdateCombined(log, baseRepo, pull, models.PendingCommitStatus, command.Apply); err != nil {
			log.Warn("unable to update commit status: %s", err)
		}
	}

	// Run apply for valid projects
	preApplyPullStatus := status
	result := runProjectCmdsWithCancellationTracker(ctx, validCmds, a.cancellationTracker, a.parallelPoolSize, a.isParallelEnabled(validCmds), a.prjCmdRunner.Apply)

	// Refresh live pull identity after apply
	finalLivePull, err := a.refreshLivePullIdentity(log, pull)
	if err != nil {
		log.Err("fetching live pull request after apply: %s", err)
		result.Error = fmt.Errorf("fetching live pull request after apply: %w", err)
		if statusErr := a.commitStatusUpdater.UpdateCombined(log, baseRepo, pull, models.FailedCommitStatus, command.Apply); statusErr != nil {
			log.Warn("unable to update commit status: %s", statusErr)
		}
		a.pullUpdater.updatePull(ctx, cmd, result)
		return
	}
	if err := livePullIdentityChangedDuringApply(livePull, finalLivePull); err != nil {
		log.Warn("apply result is stale because %s", err)
		result.Error = err
		a.publishDeferredApplyStatuses(validCmds, result, models.FailedCommitStatus)
		if statusErr := a.commitStatusUpdater.UpdateCombined(log, baseRepo, pull, models.FailedCommitStatus, command.Apply); statusErr != nil {
			log.Warn("unable to update commit status: %s", statusErr)
		}
		a.pullUpdater.updatePull(ctx, cmd, result)
		return
	}

	ctx.CommandHasErrors = result.HasErrors()
	a.pullUpdater.updatePull(ctx, cmd, result)

	pullStatus, err := a.dbUpdater.updateDB(ctx, pull, result.ProjectResults)
	if err != nil {
		log.Err("writing results: %s", err)
		return
	}

	currentPull := applyPullWithLiveIdentity(pull, finalLivePull)
	if err := applyResultStatusUpdateError(result, pullStatus, pull, currentPull, preApplyPullStatus); err != nil {
		log.Warn("not publishing apply success status because %s", err)
		ctx.CommandHasErrors = true
		a.publishDeferredApplyStatuses(validCmds, result, models.FailedCommitStatus)
		if statusErr := a.commitStatusUpdater.UpdateCombined(log, baseRepo, pull, models.FailedCommitStatus, command.Apply); statusErr != nil {
			log.Warn("unable to update commit status: %s", statusErr)
		}
		return
	}

	a.publishDeferredApplyStatuses(validCmds, result, models.SuccessCommitStatus)
	a.updateCommitStatus(ctx, pullStatus)

	if result.HasErrors() {
		return
	}

	if err := pullStatusFreshnessError(currentPull, pullStatus.Pull, "recorded apply status"); err != nil {
		log.Warn("not automerging because %s", err)
		return
	}

	if a.autoMerger.automergeEnabled(validCmds) {
		if len(a.disableAutomergeLabel) > 0 {
			labels, err := a.vcsClient.GetPullLabels(log, baseRepo, pull)
			if err != nil {
				log.Err("unable to get pull request labels so not automerging, error %s", err)
				return
			} else if slices.Contains(labels, a.disableAutomergeLabel) {
				log.Info("pull/merge request has disable automerge label %q so not automerging", a.disableAutomergeLabel)
				return
			}
		}
		a.autoMerger.automerge(ctx, pullStatus, a.autoMerger.deleteSourceBranchOnMergeEnabled(validCmds), "")
	}
}

func (a *defaultAutoApplyCommandRunner) validateAndFilterProjects(log logging.SimpleLogging, cmds []command.ProjectContext) []command.ProjectContext {
	var validCmds []command.ProjectContext
	for _, cmd := range cmds {
		// Resolve the on-disk working dir so requirements that depend on the
		// checkout (e.g. undiverged) can be evaluated. If the workspace is not
		// present, fall back to an empty path so the remaining requirement
		// checks still run.
		repoDir, err := a.workingDir.GetWorkingDir(cmd.BaseRepo, cmd.Pull, cmd.Workspace)
		if err != nil && !os.IsNotExist(err) {
			log.Warn("unable to get working dir for project %s: %s", cmd.ProjectName, err)
			repoDir = ""
		}

		failure, err := a.requirementHandler.ValidateApplyProject(repoDir, cmd)
		if err != nil {
			log.Err("validating apply requirements for project %s: %s", cmd.ProjectName, err)
			continue
		}
		if failure != "" {
			log.Info("project %s failed apply requirements: %s", cmd.ProjectName, failure)
			continue
		}
		validCmds = append(validCmds, cmd)
	}
	return validCmds
}

func (a *defaultAutoApplyCommandRunner) publishDeferredApplyStatuses(projectCmds []command.ProjectContext, result command.Result, status models.CommitStatus) {
	publisher, ok := a.prjCmdRunner.(DeferredApplyStatusPublisher)
	if !ok {
		return
	}
	publisher.PublishDeferredApplyStatuses(projectCmds, result, status)
}

func (a *defaultAutoApplyCommandRunner) updateCommitStatus(ctx *command.Context, pullStatus models.PullStatus) {
	var numSuccess int
	var numErrored int
	var numNoChanges int
	status := models.SuccessCommitStatus

	numNoChanges = pullStatus.StatusCount(models.PlannedNoChangesPlanStatus)
	numSuccess = pullStatus.StatusCount(models.AppliedPlanStatus) + numNoChanges
	numErrored = pullStatus.StatusCount(models.ErroredApplyStatus)

	if numErrored > 0 {
		status = models.FailedCommitStatus
	} else if numSuccess < len(pullStatus.Projects) {
		status = models.PendingCommitStatus
	}

	if err := a.commitStatusUpdater.UpdateCombinedCount(
		ctx.Log,
		ctx.Pull.BaseRepo,
		ctx.Pull,
		status,
		command.Apply,
		models.ProjectCounts{Success: numSuccess, Total: len(pullStatus.Projects), Errored: numErrored, NoChanges: numNoChanges},
	); err != nil {
		ctx.Log.Warn("unable to update commit status: %s", err)
	}
}

func (a *defaultAutoApplyCommandRunner) IsLocked() (bool, error) {
	lock, err := a.locker.CheckApplyLock()
	return lock.Locked, err
}

func (a *defaultAutoApplyCommandRunner) isParallelEnabled(projectCmds []command.ProjectContext) bool {
	return len(projectCmds) > 0 && projectCmds[0].ParallelApplyEnabled
}

func (a *defaultAutoApplyCommandRunner) refreshLivePullIdentity(log logging.SimpleLogging, pull models.PullRequest) (models.PullRequest, error) {
	if a.livePullHeadFetcher == nil {
		return models.PullRequest{}, nil
	}
	livePull, err := a.livePullHeadFetcher.GetLivePullIdentity(command.ProjectContext{
		Log:        log,
		Pull:       pull,
		PullStatus: nil,
		API:        false,
	})
	if err != nil {
		return models.PullRequest{}, err
	}
	if livePull.HeadCommit == "" {
		return models.PullRequest{}, fmt.Errorf("live pull request head is empty")
	}
	return livePull, nil
}

func (a *defaultAutoApplyCommandRunner) fetchUserTeams(logger logging.SimpleLogging, repo models.Repo, user *models.User) error {
	return fetchUserTeamsForTeamAllowlist(a.vcsClient, logger, repo, user)
}

func (a *defaultAutoApplyCommandRunner) checkUserPermissions(repo models.Repo, user *models.User, cmdName string) (bool, error) {
	return checkUserPermissionsForTeamAllowlist(a.vcsClient, a.teamAllowlistChecker, a.Logger, repo, user, cmdName)
}

func (a *defaultAutoApplyCommandRunner) addHierarchyTeamsForCommand(repo models.Repo, user *models.User, cmdName string) {
	a.addHierarchyTeamsForCommandForTeams(repo, user, cmdName, user.Teams)
}

func (a *defaultAutoApplyCommandRunner) addHierarchyTeamsForCommandForTeams(repo models.Repo, user *models.User, cmdName string, teams []string) {
	addHierarchyTeamsForCommandForTeams(a.vcsClient, a.teamAllowlistChecker, a.Logger, repo, user, cmdName, teams)
}

func (a *defaultAutoApplyCommandRunner) logPanics(baseRepo models.Repo, pullNum int, logger logging.SimpleLogging) {
	if err := recover(); err != nil {
		stack := recovery.Stack(3)
		logger.Err("PANIC: %s\n%s", err, stack)
		if commentErr := a.vcsClient.CreateComment(
			logger,
			baseRepo,
			pullNum,
			fmt.Sprintf("**Error: goroutine panic. This is a bug.**\n```\n%s\n%s```", err, stack),
			"",
		); commentErr != nil {
			logger.Err("unable to comment: %s", commentErr)
		}
	}
}

var _ AutoApplyCommandRunner = (*defaultAutoApplyCommandRunner)(nil)
