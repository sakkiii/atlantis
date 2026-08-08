// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package redisha

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

func TestManifestHasReplicaRoutingTopology(t *testing.T) {
	file, err := os.Open("atlantis.yaml")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	decoder := yaml.NewDecoder(file)
	resources := make(map[string]manifest)
	for {
		var resource manifest
		err := decoder.Decode(&resource)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotEmpty(t, resource.Kind)
		resources[resource.Kind+"/"+resource.Metadata.Name] = resource
	}

	require.Len(t, resources, 7)
	require.Equal(t, "None", resources["Service/atlantis-headless"].Spec.ClusterIP)
	require.Equal(t, "None", resources["Service/atlantis"].Spec.SessionAffinity)

	statefulSet := resources["StatefulSet/atlantis"]
	require.Equal(t, 3, statefulSet.Spec.Replicas)
	require.Equal(t, "atlantis-headless", statefulSet.Spec.ServiceName)
	require.Len(t, statefulSet.Spec.Template.Spec.Containers, 1)
	container := statefulSet.Spec.Template.Spec.Containers[0]
	require.Equal(t, "/readyz", container.ReadinessProbe.HTTPGet.Path)
	require.Equal(t, "/healthz", container.LivenessProbe.HTTPGet.Path)

	env := make(map[string]string)
	for _, variable := range container.Env {
		env[variable.Name] = variable.Value
	}
	require.Equal(t, "redis", env["ATLANTIS_LOCKING_DB_TYPE"])
	require.Contains(t, env["ATLANTIS_REPLICA_ADVERTISE_URL"], "atlantis-headless")
	require.NotContains(t, env, "ATLANTIS_ENABLE_REPLICA_ROUTING")
	require.NotContains(t, env, "ATLANTIS_REPLICA_ID")
	require.Contains(t, resources, "NetworkPolicy/atlantis-ingress")
}

type manifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		ClusterIP       string `yaml:"clusterIP"`
		SessionAffinity string `yaml:"sessionAffinity"`
		Replicas        int    `yaml:"replicas"`
		ServiceName     string `yaml:"serviceName"`
		Template        struct {
			Spec struct {
				Containers []struct {
					Env []struct {
						Name  string `yaml:"name"`
						Value string `yaml:"value"`
					} `yaml:"env"`
					ReadinessProbe struct {
						HTTPGet struct {
							Path string `yaml:"path"`
						} `yaml:"httpGet"`
					} `yaml:"readinessProbe"`
					LivenessProbe struct {
						HTTPGet struct {
							Path string `yaml:"path"`
						} `yaml:"httpGet"`
					} `yaml:"livenessProbe"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}
