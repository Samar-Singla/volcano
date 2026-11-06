/*
Copyright 2025 The Volcano Authors.

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

package allocate

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// BenchmarkAllocateGang measures the allocate action with all members required.
// Session setup, asynchronous bind verification and cleanup are outside the timer.
func BenchmarkAllocateGang(b *testing.B) {
	for _, n := range []int{1000, 2000, 4000} {
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				func() {
					tc := uthelper.TestCommonStruct{
						Name:      "gang-readiness",
						Plugins:   map[string]framework.PluginBuilder{gang.PluginName: gang.New, predicates.PluginName: predicates.New},
						PodGroups: []*schedulingv1.PodGroup{util.BuildPodGroup("pg", "ns", "q", int32(n), map[string]int32{"worker": int32(n)}, schedulingv1.PodGroupInqueue)},
						Queues:    []*schedulingv1.Queue{util.BuildQueue("q", 1, nil)},
					}
					// 100 requested CPUs per node keeps node density realistic.
					for j := 0; j < (n+99)/100; j++ {
						tc.Nodes = append(tc.Nodes, util.BuildNode(fmt.Sprintf("node-%d", j), api.BuildResourceList("100", "200Gi", api.ScalarResource{Name: "pods", Value: "110"}), nil))
					}
					for j := 0; j < n; j++ {
						tc.Pods = append(tc.Pods, util.BuildPod("ns", fmt.Sprintf("pod-%d", j), "", v1.PodPending, api.BuildResourceList("1", "1Gi"), "pg", map[string]string{"volcano.sh/task-spec": "worker"}, nil))
					}
					binder := util.NewFakeBinder(n)
					// Discard events so the mock recorder cannot block large gangs.
					sc := cache.NewCustomMockSchedulerCache("gang-benchmark", binder, util.NewFakeEvictor(0), &util.FakeStatusUpdater{}, nil, &record.FakeRecorder{})
					stop := make(chan struct{})
					defer close(stop)
					sc.Run(stop)
					sc.WaitForCacheSync(stop)
					for _, node := range tc.Nodes {
						sc.AddOrUpdateNode(node)
					}
					for _, pod := range tc.Pods {
						sc.AddPod(pod)
					}
					for _, pg := range tc.PodGroups {
						sc.AddPodGroupV1beta1(pg)
					}
					for _, queue := range tc.Queues {
						sc.AddQueueV1beta1(queue)
					}
					uthelper.RegisterPlugins(tc.Plugins)
					defer framework.CleanupPluginBuilders()
					ssn := framework.OpenSession(sc, []conf.Tier{{Plugins: []conf.PluginOption{
						{Name: gang.PluginName, EnabledJobReady: ptr.To(true)},
						{Name: predicates.PluginName, EnabledPredicate: ptr.To(true)},
					}}}, nil)
					defer framework.CloseSession(ssn)
					b.StartTimer()
					New().Execute(ssn)
					b.StopTimer()
					for j := 0; j < n; j++ {
						select {
						case <-binder.Channel:
						case <-time.After(5 * time.Second):
							b.Fatalf("only %d/%d pods bound", j, n)
						}
					}
					if binder.Length() != n {
						b.Fatalf("bound %d unique pods, want %d", binder.Length(), n)
					}
				}()
			}
		})
	}
}
