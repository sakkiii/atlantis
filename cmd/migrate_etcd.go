// Copyright 2017 HootSuite Media Inc.
// SPDX-License-Identifier: Apache-2.0
// Modified hereafter by contributors to runatlantis/atlantis.
//
// This file implements the offline "migrate-etcd" operator subcommand: a
// one-shot cutover that exports persistent BoltDB state (project locks, pull
// statuses, and global command locks) and imports it into a fresh, empty etcd
// namespace using the etcd package's create-only Migrator (design §"Migration
// and rollback"). It is offline by construction: Atlantis must be drained and
// BoltDB is opened read-only. It never dual-writes and never migrates ownership
// or session keys. On any conflict the migration aborts and the operator must
// discard and recreate the target namespace.
package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/runatlantis/atlantis/server/core/boltdb"
	"github.com/runatlantis/atlantis/server/core/etcd"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// migrateEtcd flag names. They mirror the etcd client flags on the server
// command so operators use one consistent vocabulary, but they are defined
// locally on this subcommand.
const (
	migrateDataDirFlag          = "data-dir"
	migrateDeploymentIDFlag     = "etcd-deployment-id"
	migrateNamespaceFlag        = "etcd-namespace"
	migrateEndpointsFlag        = "etcd-endpoints"
	migrateCAFileFlag           = "etcd-ca-file"
	migrateCertFileFlag         = "etcd-cert-file"
	migrateKeyFileFlag          = "etcd-key-file"
	migrateServerNameFlag       = "etcd-server-name"
	migrateUsernameFlag         = "etcd-username"
	migratePasswordFileFlag     = "etcd-password-file" // nolint: gosec
	migrateRequestTimeoutFlag   = "etcd-request-timeout"
	migrateStartupTimeoutFlag   = "etcd-startup-timeout"
	migrateAllowInsecureDevFlag = "etcd-allow-insecure-dev"
	migrateDryRunFlag           = "dry-run"

	migrateDefaultRequestTimeout = "5s"
	migrateDefaultStartupTimeout = "5m"
)

// lockableCommands is the closed set of command names whose global locks are
// persisted in BoltDB and therefore migrated. Autoplan is deliberately omitted
// because it shares its string identity ("plan") with Plan.
var lockableCommands = []command.Name{
	command.Apply,
	command.Plan,
	command.Unlock,
	command.PolicyCheck,
	command.ApprovePolicies,
	command.Version,
	command.Import,
	command.State,
	command.Cancel,
}

// MigrateEtcdCmd is the offline BoltDB->etcd migration subcommand.
type MigrateEtcdCmd struct{}

// Init returns the runnable cobra command.
func (m *MigrateEtcdCmd) Init() *cobra.Command {
	v := viper.New()

	c := &cobra.Command{
		Use:   "migrate-etcd",
		Short: "Offline one-shot migration of persistent BoltDB state into a fresh etcd namespace",
		Long: "Exports persistent project locks, pull statuses, and global command locks from a BoltDB " +
			"data directory and imports them into an empty external etcd namespace using create-only " +
			"transactions. Atlantis must be drained first; BoltDB is opened read-only and etcd is never " +
			"dual-written. Ownership and session keys are never migrated. Use --dry-run to preview.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Bind after parsing so flags win over env vars, env vars win over
			// defaults, matching the server command's precedence.
			if err := v.BindPFlags(cmd.Flags()); err != nil {
				return err
			}
			return m.run(cmd.Context(), cmd.OutOrStdout(), v)
		},
	}

	fs := c.Flags()
	fs.String(migrateDataDirFlag, "", "Path to the source BoltDB data directory (contains atlantis.db). Required.")
	fs.String(migrateDeploymentIDFlag, "", "Stable deployment identity recorded in the etcd namespace. Required.")
	fs.String(migrateNamespaceFlag, "", "etcd key namespace to migrate into. Defaults to "+etcd.DefaultNamespace+".")
	fs.String(migrateEndpointsFlag, "", "Comma-separated https etcd client endpoints, ex. https://member-0:2379,https://member-1:2379.")
	fs.String(migrateCAFileFlag, "", "Path to the trusted CA certificate for verifying the etcd client listener.")
	fs.String(migrateCertFileFlag, "", "Path to the client certificate presented to etcd.")
	fs.String(migrateKeyFileFlag, "", "Path to the private key for the etcd client certificate.")
	fs.String(migrateServerNameFlag, "", "Expected server name for etcd TLS hostname verification.")
	fs.String(migrateUsernameFlag, "", "etcd RBAC username. When set, requires --etcd-password-file; mTLS remains mandatory.")
	fs.String(migratePasswordFileFlag, "", "Path to a file containing the etcd RBAC password. Required when --etcd-username is set.")
	fs.String(migrateRequestTimeoutFlag, migrateDefaultRequestTimeout, "Timeout bounding individual etcd RPCs, ex. 5s.")
	fs.String(migrateStartupTimeoutFlag, migrateDefaultStartupTimeout, "Timeout bounding initial etcd connectivity, ex. 5m.")
	fs.Bool(migrateAllowInsecureDevFlag, false, "Permit insecure (non-TLS, loopback-only) etcd access. Development only; never in production.")
	fs.Bool(migrateDryRunFlag, false, "Read and count the source records and print what would be migrated without writing to etcd.")

	// Accept ATLANTIS_-prefixed env vars, mirroring the server command.
	v.SetEnvPrefix("ATLANTIS")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	return c
}

// run performs the migration. It never mutates the source: BoltDB is opened and
// read only, and nothing is written to etcd until every source record has been
// read and counted.
func (m *MigrateEtcdCmd) run(ctx context.Context, out io.Writer, v *viper.Viper) error {
	if ctx == nil {
		ctx = context.Background()
	}

	dataDir := strings.TrimSpace(v.GetString(migrateDataDirFlag))
	if dataDir == "" {
		return fmt.Errorf("--%s is required", migrateDataDirFlag)
	}

	// 1. Open BoltDB (read-only usage: we only call read methods).
	db, err := boltdb.New(dataDir)
	if err != nil {
		return fmt.Errorf("opening BoltDB at %q: %w", dataDir, err)
	}
	defer db.Close() // nolint: errcheck

	// 2. Read the persistent state.
	locks, err := db.List()
	if err != nil {
		return fmt.Errorf("reading project locks: %w", err)
	}
	pulls, err := db.ListPullStatuses()
	if err != nil {
		return fmt.Errorf("reading pull statuses: %w", err)
	}

	type globalLock struct {
		name string
		lock command.Lock
	}
	var globalLocks []globalLock
	for _, name := range lockableCommands {
		l, err := db.CheckCommandLock(name)
		if err != nil {
			return fmt.Errorf("reading global %q lock: %w", name.String(), err)
		}
		if l != nil {
			globalLocks = append(globalLocks, globalLock{name: name.String(), lock: *l})
		}
	}

	// 3. Compute the expected total.
	total := len(locks) + len(pulls) + len(globalLocks)

	fmt.Fprintf(out, "Source BoltDB: %s\n", dataDir)
	fmt.Fprintf(out, "  project locks:  %d\n", len(locks))
	fmt.Fprintf(out, "  pull statuses:  %d\n", len(pulls))
	fmt.Fprintf(out, "  global locks:   %d\n", len(globalLocks))
	fmt.Fprintf(out, "  total records:  %d\n", total)

	// 4. Dry run: print a per-record summary and exit without touching etcd.
	if v.GetBool(migrateDryRunFlag) {
		fmt.Fprintln(out, "\n--dry-run: no data will be written to etcd. Records that WOULD be migrated:")
		for _, lock := range locks {
			fmt.Fprintf(out, "  [project-lock] host=%s repo=%s path=%s project=%s workspace=%s pull=%d\n",
				lock.Pull.BaseRepo.VCSHost.Hostname, lock.Project.RepoFullName, lock.Project.Path,
				lock.Project.ProjectName, lock.Workspace, lock.Pull.Num)
		}
		for _, status := range pulls {
			fmt.Fprintf(out, "  [pull-status] host=%s repo=%s pull=%d projects=%d\n",
				status.Pull.BaseRepo.VCSHost.Hostname, status.Pull.BaseRepo.FullName, status.Pull.Num,
				len(status.Projects))
		}
		for _, g := range globalLocks {
			fmt.Fprintf(out, "  [global-lock] name=%s\n", g.name)
		}
		return nil
	}

	// 5. Live migration into a fresh, empty etcd namespace.
	deploymentID := strings.TrimSpace(v.GetString(migrateDeploymentIDFlag))
	if deploymentID == "" {
		return fmt.Errorf("--%s is required for a live migration", migrateDeploymentIDFlag)
	}

	cfg, err := etcd.BuildConfig(etcd.Settings{
		Mode:             string(etcd.ModeExternal),
		DeploymentID:     deploymentID,
		Namespace:        v.GetString(migrateNamespaceFlag),
		Endpoints:        v.GetString(migrateEndpointsFlag),
		CAFile:           v.GetString(migrateCAFileFlag),
		CertFile:         v.GetString(migrateCertFileFlag),
		KeyFile:          v.GetString(migrateKeyFileFlag),
		ServerName:       v.GetString(migrateServerNameFlag),
		Username:         v.GetString(migrateUsernameFlag),
		PasswordFile:     v.GetString(migratePasswordFileFlag),
		RequestTimeout:   v.GetString(migrateRequestTimeoutFlag),
		StartupTimeout:   v.GetString(migrateStartupTimeoutFlag),
		AllowInsecureDev: v.GetBool(migrateAllowInsecureDevFlag),
	})
	if err != nil {
		return fmt.Errorf("building etcd config: %w", err)
	}

	backend, err := etcd.NewExternal(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connecting to etcd: %w", err)
	}
	defer backend.Close() // nolint: errcheck

	keys := etcd.NewKeyspace(cfg.Namespace)
	sourceIdentity := "boltdb:" + dataDir

	migrator, err := etcd.BeginMigration(ctx, backend.Client().KV, keys, deploymentID, sourceIdentity, total)
	if err != nil {
		return fmt.Errorf("beginning migration (target namespace must be genuinely empty): %w", err)
	}

	fmt.Fprintf(out, "\nBegan migration %s into namespace %s\n", migrator.MigrationID(), cfg.Namespace)

	// Import every record with create-only transactions. Any conflict aborts.
	for _, lock := range locks {
		scope := etcd.ProjectScope{
			VCSHostname: lock.Pull.BaseRepo.VCSHost.Hostname,
			Repository:  lock.Project.RepoFullName,
			Path:        lock.Project.Path,
			Project:     lock.Project.ProjectName,
			Workspace:   lock.Workspace,
		}
		if err := migrator.ImportProjectLock(ctx, scope, lock); err != nil {
			return abortMigration(out, migrator, cfg.Namespace, fmt.Errorf("importing project lock: %w", err))
		}
	}
	for _, status := range pulls {
		scope := etcd.PullScope{
			VCSHostname: status.Pull.BaseRepo.VCSHost.Hostname,
			Repository:  status.Pull.BaseRepo.FullName,
			PullNum:     status.Pull.Num,
		}
		if err := migrator.ImportPullStatus(ctx, scope, status); err != nil {
			return abortMigration(out, migrator, cfg.Namespace, fmt.Errorf("importing pull status: %w", err))
		}
	}
	for _, g := range globalLocks {
		if err := migrator.ImportGlobalLock(ctx, g.name, g.lock); err != nil {
			return abortMigration(out, migrator, cfg.Namespace, fmt.Errorf("importing global %q lock: %w", g.name, err))
		}
	}

	// Verify count + checksum and atomically complete in one transaction.
	epoch, err := migrator.Complete(ctx)
	if err != nil {
		return abortMigration(out, migrator, cfg.Namespace, fmt.Errorf("completing migration: %w", err))
	}

	fmt.Fprintf(out, "\nMigration complete.\n")
	fmt.Fprintf(out, "  migration id:      %s\n", migrator.MigrationID())
	fmt.Fprintf(out, "  records imported:  %d\n", migrator.Count())
	fmt.Fprintf(out, "  coordination epoch: %s\n", epoch)
	return nil
}

// abortMigration reports a failed migration and instructs the operator to
// discard the target namespace, per the design's "interrupted import" rule: the
// namespace is left with an in-progress manifest and normal startup will refuse
// it until it is discarded and recreated.
func abortMigration(out io.Writer, migrator *etcd.Migrator, namespace string, cause error) error {
	fmt.Fprintf(out, "\nMigration %s ABORTED: %v\n", migrator.MigrationID(), cause)
	fmt.Fprintf(out, "The target namespace %s is now in an incomplete state and must be discarded and recreated before retrying.\n", namespace)
	return cause
}
