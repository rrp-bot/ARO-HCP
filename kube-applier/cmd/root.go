// Copyright 2026 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"k8s.io/klog/v2"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/internal/signal"
	"github.com/Azure/ARO-HCP/internal/utils"
	"github.com/Azure/ARO-HCP/internal/version"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/app"
)

// KubeApplierRootCmdFlags collects the user-facing flags for the kube-applier
// binary. Required values must be supplied as flags; the binary does not read
// from environment variables so a misconfigured pod fails fast instead of
// silently picking up a stale operator-shell value.
type KubeApplierRootCmdFlags struct {
	Kubeconfig                  string
	KubeNamespace               string
	ManagementClusterResourceID string
	MetricsServerListenAddress  string
	HealthzServerListenAddress  string
	LeaderElectionID            string
	LogVerbosity                int
	ExitOnPanic                 bool

	// Backend selects the storage implementation.
	// Supported values: "cosmos" (default), "dynamodb".
	Backend string

	// Cosmos-specific flags — required when --backend=cosmos (the default).
	AzureCosmosDBName        string
	AzureCosmosDBURL         string
	AzureCosmosContainerName string

	// DynamoDB-specific flags — required when --backend=dynamodb.
	DynamoTableName string
	AWSRegion       string
}

func (f *KubeApplierRootCmdFlags) AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.Kubeconfig, "kubeconfig", f.Kubeconfig,
		"Absolute path to the kubeconfig file. Empty selects the in-cluster config.")
	cmd.Flags().StringVar(&f.KubeNamespace, "namespace", f.KubeNamespace,
		"Kubernetes namespace that hosts the leader-election lease.")
	cmd.Flags().StringVar(&f.ManagementClusterResourceID, "management-cluster", f.ManagementClusterResourceID,
		"ResourceID of the management cluster this pod runs in. Used as the storage shard/partition key.")
	cmd.Flags().StringVar(&f.Backend, "backend", f.Backend,
		`Storage backend to use. One of "cosmos" (default) or "dynamodb".`)

	// Cosmos flags
	cmd.Flags().StringVar(&f.AzureCosmosDBName, "cosmos-name", f.AzureCosmosDBName,
		"Cosmos database name. Required when --backend=cosmos.")
	cmd.Flags().StringVar(&f.AzureCosmosDBURL, "cosmos-url", f.AzureCosmosDBURL,
		"Cosmos database URL. Required when --backend=cosmos.")
	cmd.Flags().StringVar(&f.AzureCosmosContainerName, "cosmos-container", f.AzureCosmosContainerName,
		"Cosmos container name. Required when --backend=cosmos.")

	// DynamoDB flags
	cmd.Flags().StringVar(&f.DynamoTableName, "dynamodb-table", f.DynamoTableName,
		"DynamoDB table name. Required when --backend=dynamodb.")
	cmd.Flags().StringVar(&f.AWSRegion, "aws-region", f.AWSRegion,
		"AWS region for DynamoDB. Required when --backend=dynamodb.")

	cmd.Flags().StringVar(&f.MetricsServerListenAddress, "metrics-listen-address", f.MetricsServerListenAddress,
		"Address on which to expose Prometheus metrics.")
	cmd.Flags().StringVar(&f.HealthzServerListenAddress, "healthz-listen-address", f.HealthzServerListenAddress,
		"Address on which to expose the /healthz endpoint.")
	cmd.Flags().StringVar(&f.LeaderElectionID, "leader-election-id", f.LeaderElectionID,
		"Lease name used for leader election within --namespace.")
	cmd.Flags().IntVar(&f.LogVerbosity, "log-verbosity", f.LogVerbosity,
		"Log verbosity. 0 is INFO; higher values are more verbose.")
	cmd.Flags().BoolVar(&f.ExitOnPanic, "exit-on-panic", f.ExitOnPanic,
		"If set, the process exits on any goroutine panic via apimachinery's HandleCrash.")

	for _, name := range []string{"namespace", "management-cluster"} {
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(fmt.Errorf("MarkFlagRequired(%q): %w", name, err))
		}
	}
}

func (f *KubeApplierRootCmdFlags) validate() error {
	if len(f.ManagementClusterResourceID) == 0 {
		return utils.TrackError(fmt.Errorf("--management-cluster must not be empty"))
	}
	if len(f.KubeNamespace) == 0 {
		return utils.TrackError(fmt.Errorf("--namespace must not be empty"))
	}
	if len(f.LeaderElectionID) == 0 {
		return utils.TrackError(fmt.Errorf("--leader-election-id must not be empty"))
	}
	if f.LogVerbosity < 0 {
		return utils.TrackError(fmt.Errorf("--log-verbosity must be >= 0"))
	}

	switch app.BackendType(f.Backend) {
	case app.BackendTypeCosmos:
		if len(f.AzureCosmosDBName) == 0 {
			return utils.TrackError(fmt.Errorf("--cosmos-name must not be empty when --backend=cosmos"))
		}
		if len(f.AzureCosmosDBURL) == 0 {
			return utils.TrackError(fmt.Errorf("--cosmos-url must not be empty when --backend=cosmos"))
		}
		if len(f.AzureCosmosContainerName) == 0 {
			return utils.TrackError(fmt.Errorf("--cosmos-container must not be empty when --backend=cosmos"))
		}
	case app.BackendTypeDynamoDB:
		if len(f.DynamoTableName) == 0 {
			return utils.TrackError(fmt.Errorf("--dynamodb-table must not be empty when --backend=dynamodb"))
		}
		if len(f.AWSRegion) == 0 {
			return utils.TrackError(fmt.Errorf("--aws-region must not be empty when --backend=dynamodb"))
		}
	default:
		return utils.TrackError(fmt.Errorf("--backend must be %q or %q, got %q",
			app.BackendTypeCosmos, app.BackendTypeDynamoDB, f.Backend))
	}

	return nil
}

// ToKubeApplierOptions resolves flags into the wired Options that the app
// layer consumes.
func (f *KubeApplierRootCmdFlags) ToKubeApplierOptions(ctx context.Context, cmd *cobra.Command) (*app.Options, error) {
	kubeconfig, err := app.NewKubeconfig(f.Kubeconfig)
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to create Kubernetes configuration: %w", err))
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to get hostname: %w", err))
	}
	leaderElectionLock, err := app.NewLeaderElectionLock(hostname, kubeconfig, f.KubeNamespace, f.LeaderElectionID)
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to create leader election lock: %w", err))
	}

	managementClusterResourceID, err := azcorearm.ParseResourceID(f.ManagementClusterResourceID)
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to parse management cluster resource ID: %w", err))
	}

	kubeApplierDBClient, err := app.NewKubeApplierDBClient(ctx, app.BackendConfig{
		Backend:                     app.BackendType(f.Backend),
		CosmosDBURL:                 f.AzureCosmosDBURL,
		CosmosDBName:                f.AzureCosmosDBName,
		CosmosContainerName:         f.AzureCosmosContainerName,
		DynamoTableName:             f.DynamoTableName,
		AWSRegion:                   f.AWSRegion,
		ManagementClusterResourceID: managementClusterResourceID,
	})
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to create kube-applier storage client: %w", err))
	}

	dyn, err := app.NewDynamicClient(kubeconfig)
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to create dynamic client: %w", err))
	}

	return &app.Options{
		ManagementCluster:          managementClusterResourceID,
		LeaderElectionLock:         leaderElectionLock,
		KubeApplierDBClient:        kubeApplierDBClient,
		DynamicClient:              dyn,
		MetricsServerListenAddress: f.MetricsServerListenAddress,
		HealthzServerListenAddress: f.HealthzServerListenAddress,
		ExitOnPanic:                f.ExitOnPanic,
	}, nil
}

func NewKubeApplierRootCmdFlags() *KubeApplierRootCmdFlags {
	return &KubeApplierRootCmdFlags{
		Backend:                    string(app.BackendTypeCosmos),
		MetricsServerListenAddress: ":8081",
		HealthzServerListenAddress: ":8083",
		LeaderElectionID:           "kube-applier",
		LogVerbosity:               0,
		ExitOnPanic:                true,
	}
}

func NewCmdRoot() *cobra.Command {
	processName := filepath.Base(os.Args[0])

	flags := NewKubeApplierRootCmdFlags()

	cmd := &cobra.Command{
		Use:   processName,
		Args:  cobra.NoArgs,
		Short: app.AppShortDescriptionName,
		Long: fmt.Sprintf(`%s

	The kube-applier reconciles ApplyDesire, DeleteDesire, and ReadDesire
	documents stored in a backend database against the management cluster's
	local kube-apiserver. Supported backends: cosmos (default), dynamodb.

	# Run against Cosmos (default)
	%s --management-cluster ${MANAGEMENT_CLUSTER} \
		--cosmos-container ${CONTAINER_NAME} \
		--cosmos-name ${DB_NAME} --cosmos-url ${DB_URL} \
		--namespace ${RP_NAMESPACE}

	# Run against DynamoDB
	%s --backend=dynamodb \
		--management-cluster ${MANAGEMENT_CLUSTER} \
		--dynamodb-table ${TABLE_NAME} --aws-region ${REGION} \
		--namespace ${RP_NAMESPACE}
`, app.AppShortDescriptionName, processName, processName),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := RunRootCmd(cmd, flags)
			if err != nil {
				return utils.TrackError(fmt.Errorf("failed to run: %w", err))
			}
			return nil
		},
		SilenceErrors: true,
	}

	cmd.SetErrPrefix(cmd.Short + " error:")
	cmd.Version = version.CommitSHA
	flags.AddFlags(cmd)

	return cmd
}

func RunRootCmd(cmd *cobra.Command, flags *KubeApplierRootCmdFlags) error {
	if err := flags.validate(); err != nil {
		return utils.TrackError(fmt.Errorf("flags validation failed: %w", err))
	}

	ctx := signal.SetupSignalContext()

	handlerOptions := &slog.HandlerOptions{Level: slog.Level(flags.LogVerbosity * -1), AddSource: true}
	slogJSONHandler := slog.NewJSONHandler(os.Stdout, handlerOptions)
	logger := logr.FromSlogHandler(slogJSONHandler)
	ctx = utils.ContextWithLogger(ctx, logger)
	klog.SetLogger(logger)

	options, err := flags.ToKubeApplierOptions(ctx, cmd)
	if err != nil {
		return utils.TrackError(fmt.Errorf("failed to convert flags to options: %w", err))
	}

	if err := options.Run(ctx); err != nil {
		return utils.TrackError(fmt.Errorf("failed to run kube-applier: %w", err))
	}
	return nil
}
