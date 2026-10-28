/*
Copyright 2022 The Kruise Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"fmt"
	"net/http"

	gamekruiseiov1alpha1 "github.com/openkruise/kruise-game/apis/v1alpha1"
	"github.com/openkruise/kruise-game/cloudprovider/manager"
	"github.com/openkruise/kruise-game/pkg/util"
	admissionv1 "k8s.io/api/admission/v1"
	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type GssValidaatingHandler struct {
	Client               client.Client
	decoder              admission.Decoder
	CloudProviderManager *manager.ProviderManager
}

func (gvh *GssValidaatingHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	gss := &gamekruiseiov1alpha1.GameServerSet{}
	err := gvh.decoder.Decode(req, gss)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Enforce the new validation only for opted-in GSS objects; preserve the
	// existing admission behavior for objects without completionPolicy.
	if allowed, reason := validatingGss(gss, gvh.Client); !allowed && util.HasCompletionPolicy(gss) {
		return admission.ValidationResponse(allowed, reason)
	}

	switch req.Operation {
	case admissionv1.Update:
		newGss := gss.DeepCopy()
		oldGss := &gamekruiseiov1alpha1.GameServerSet{}
		err := gvh.decoder.DecodeRaw(req.OldObject, oldGss)
		if err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
		return validatingUpdate(newGss, oldGss)
	case admissionv1.Create:
		newGss := gss.DeepCopy()
		return validatingCreate(newGss, gvh.CloudProviderManager)
	}

	return admission.ValidationResponse(true, "pass validating")
}

func validatingGss(gss *gamekruiseiov1alpha1.GameServerSet, client client.Client) (bool, string) {
	// validate reserveGameServerIds
	vset := sets.Set[int]{}

	validate := func(ids intstr.IntOrString) (bool, string) {
		switch ids.Type {
		case intstr.Int:
			id := ids.IntVal
			if id < 0 {
				return false, fmt.Sprintf("reserveGameServerIds should be greater or equal to 0. Now it is %d", id)
			}
			if vset.Has(int(id)) {
				return false, fmt.Sprintf("reserveGameServerIds should not be repeat. Now it is %d", id)
			}
			vset.Insert(int(id))
		case intstr.String:
			start, end, err := util.ParseRange(ids.StrVal)
			if err != nil {
				return false, fmt.Sprintf("invalid range reserveGameServerIds found, an empty slice will be returned: %s", ids.StrVal)
			}
			if start < 0 {
				return false, fmt.Sprintf("reserveGameServerIds should be greater or equal to 0. Now it is %d", start)
			}
			if end < 0 {
				return false, fmt.Sprintf("reserveGameServerIds should be greater or equal to 0. Now it is %d", end)
			}
			if start > end {
				return false, fmt.Sprintf("invalid range reserveGameServerIds found, an empty slice will be returned: %s", ids.StrVal)
			}
			if vset.Has(int(start)) || vset.Has(int(end)) {
				return false, fmt.Sprintf("reserveGameServerIds should not be repeat. Now it is %d-%d", start, end)
			}
			for i := start; i <= end; i++ {
				if vset.Has(int(i)) {
					return false, fmt.Sprintf("reserveGameServerIds should not be repeat. Now it is %d-%d", start, end)
				}
				vset.Insert(int(i))
			}
		}
		return true, ""
	}

	for _, id := range gss.Spec.ReserveGameServerIds {
		if ok, reason := validate(id); !ok {
			return false, reason
		}
	}
	if ok, reason := validateCompletionPolicy(gss); !ok {
		return false, reason
	}

	return true, "general validating success"
}

func validateCompletionPolicy(gss *gamekruiseiov1alpha1.GameServerSet) (bool, string) {
	ru := gss.Spec.UpdateStrategy.RollingUpdate
	if ru == nil || ru.CompletionPolicy == nil {
		return true, ""
	}
	if window := ru.CompletionPolicy.ObservationWindowSeconds; window != nil && *window <= 0 {
		return false, "completionPolicy.observationWindowSeconds must be positive"
	}
	if typ := gss.Spec.UpdateStrategy.Type; typ != "" && typ != apps.RollingUpdateStatefulSetStrategyType {
		return false, "completionPolicy requires RollingUpdate updateStrategy"
	}
	if gss.Spec.Replicas == nil {
		return false, "completionPolicy requires replicas"
	}
	if ru.Partition != nil && (*ru.Partition < 0 || *ru.Partition > *gss.Spec.Replicas) {
		return false, "rollingUpdate.partition must be between 0 and replicas"
	}
	if ru.MaxUnavailable != nil {
		n, err := intstr.GetScaledValueFromIntOrPercent(ru.MaxUnavailable, int(*gss.Spec.Replicas), false)
		if err != nil || n < 0 {
			return false, "rollingUpdate.maxUnavailable must be a non-negative integer or percentage"
		}
	}
	if len(ru.CompletionPolicy.ServiceQualities) == 0 {
		return false, "completionPolicy.serviceQualities must not be empty"
	}
	available := make(map[string][]gamekruiseiov1alpha1.ServiceQuality, len(gss.Spec.ServiceQualities))
	for _, sq := range gss.Spec.ServiceQualities {
		available[sq.Name] = append(available[sq.Name], sq)
	}
	containers := make(map[string]struct{}, len(gss.Spec.GameServerTemplate.Spec.Containers))
	for _, container := range gss.Spec.GameServerTemplate.Spec.Containers {
		containers[container.Name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(ru.CompletionPolicy.ServiceQualities))
	for _, condition := range ru.CompletionPolicy.ServiceQualities {
		if condition.Name == "" {
			return false, "completionPolicy.serviceQualities name must not be empty"
		}
		if _, exists := seen[condition.Name]; exists {
			return false, fmt.Sprintf("completionPolicy.serviceQualities has duplicate name %q", condition.Name)
		}
		seen[condition.Name] = struct{}{}
		if len(available[condition.Name]) != 1 {
			return false, fmt.Sprintf("completionPolicy.serviceQualities name %q must reference exactly one ServiceQuality", condition.Name)
		}
		containerName := available[condition.Name][0].ContainerName
		if _, exists := containers[containerName]; !exists {
			return false, fmt.Sprintf("ServiceQuality %q must probe an existing container", condition.Name)
		}
	}
	return true, ""
}

func validatingUpdate(newGss, oldGss *gamekruiseiov1alpha1.GameServerSet) admission.Response {
	if oldGss.Spec.Network != nil && newGss.Spec.Network != nil {
		if oldGss.Spec.Network.NetworkType != "" && newGss.Spec.Network.NetworkType != oldGss.Spec.Network.NetworkType {
			return admission.ValidationResponse(false, "change network type is not allowed")
		}
	}
	if util.IsServiceQualityGatedUpdateActive(oldGss) {
		oldPolicy := oldGss.Spec.UpdateStrategy.RollingUpdate.CompletionPolicy
		newStrategy := newGss.Spec.UpdateStrategy.RollingUpdate
		if newStrategy == nil || newStrategy.CompletionPolicy == nil {
			return admission.ValidationResponse(false, "cannot remove completionPolicy during an active update")
		}
		if ptr.Deref(oldGss.Spec.UpdateStrategy.RollingUpdate.Partition, 0) != ptr.Deref(newStrategy.Partition, 0) {
			return admission.ValidationResponse(false, "rollingUpdate.partition cannot change during an active update")
		}
		if equality.Semantic.DeepEqual(oldGss.Spec.GameServerTemplate, newGss.Spec.GameServerTemplate) {
			if !completionPoliciesEqual(oldPolicy, newStrategy.CompletionPolicy) ||
				!referencedServiceQualitiesEqual(oldGss, newGss, oldPolicy) {
				return admission.ValidationResponse(false, "observation window, completion conditions, and referenced ServiceQuality configuration cannot change without a new PodTemplate during an active update")
			}
		}
	}
	return admission.ValidationResponse(true, "validatingUpdate success")
}

func completionPoliciesEqual(a, b *gamekruiseiov1alpha1.UpdateCompletionPolicy) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a.ServiceQualities) != len(b.ServiceQualities) {
		return false
	}
	if !equality.Semantic.DeepEqual(a.ObservationWindowSeconds, b.ObservationWindowSeconds) {
		return false
	}
	byName := make(map[string]gamekruiseiov1alpha1.ServiceQualityCompletionCondition, len(a.ServiceQualities))
	for _, condition := range a.ServiceQualities {
		byName[condition.Name] = condition
	}
	for _, condition := range b.ServiceQualities {
		if old, exists := byName[condition.Name]; !exists || old != condition {
			return false
		}
	}
	return true
}

func referencedServiceQualitiesEqual(oldGss, newGss *gamekruiseiov1alpha1.GameServerSet, policy *gamekruiseiov1alpha1.UpdateCompletionPolicy) bool {
	for _, condition := range policy.ServiceQualities {
		var oldSQ, newSQ *gamekruiseiov1alpha1.ServiceQuality
		for i := range oldGss.Spec.ServiceQualities {
			if oldGss.Spec.ServiceQualities[i].Name == condition.Name {
				oldSQ = &oldGss.Spec.ServiceQualities[i]
				break
			}
		}
		for i := range newGss.Spec.ServiceQualities {
			if newGss.Spec.ServiceQualities[i].Name == condition.Name {
				newSQ = &newGss.Spec.ServiceQualities[i]
				break
			}
		}
		if oldSQ == nil || newSQ == nil || !equality.Semantic.DeepEqual(oldSQ, newSQ) {
			return false
		}
	}
	return true
}

func validatingCreate(gss *gamekruiseiov1alpha1.GameServerSet, cpm *manager.ProviderManager) admission.Response {
	if gss.Spec.Network != nil {
		if gss.Spec.Network.NetworkType == "" {
			return admission.ValidationResponse(false, "network type is required")
		}
		if pn := listPluginNames(cpm); !util.IsStringInList(gss.Spec.Network.NetworkType, pn) {
			return admission.ValidationResponse(false, fmt.Sprintf("network type must be one of %v", pn))
		}
	}
	return admission.ValidationResponse(true, "validatingCreate success")
}

func listPluginNames(cpm *manager.ProviderManager) []string {
	var pluginNames []string
	for _, cp := range cpm.CloudProviders {
		plugins, _ := cp.ListPlugins()
		for _, p := range plugins {
			pluginNames = append(pluginNames, p.Name())
		}
	}
	return pluginNames
}
