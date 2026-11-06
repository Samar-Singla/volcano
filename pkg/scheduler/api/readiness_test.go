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

package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"volcano.sh/apis/pkg/apis/scheduling"
)

// Check against task state rather than maintained indexes after every transition.
func checkReadinessAgainstTasks(t *testing.T, job *JobInfo) {
	t.Helper()
	roles := map[string]int32{}
	var pending, ready int32
	for _, task := range job.Tasks {
		switch task.Status {
		case Allocated, Binding, Bound, Running, Succeeded:
			ready++
			roles[task.TaskRole]++
		case Pending:
			if task.BestEffort {
				pending++
			}
			if task.InitResreq.IsEmpty() {
				roles[task.TaskRole]++
			}
		}
	}
	want := true
	if job.MinAvailable >= job.TaskMinAvailableTotal {
		for role, min := range job.TaskMinAvailable {
			if roles[role] < min {
				want = false
			}
		}
	}
	assert.Equal(t, want, job.CheckTaskReady())
	assert.Equal(t, pending, job.PendingBestEffortTaskNum())
	assert.Equal(t, ready+pending >= job.MinAvailable, job.IsReady())
	for _, sub := range job.SubJobs {
		var subPending, subReady int32
		for _, task := range sub.Tasks {
			switch task.Status {
			case Allocated, Binding, Bound, Running, Succeeded:
				subReady++
			case Pending:
				if task.BestEffort {
					subPending++
				}
			}
		}
		assert.Equal(t, subPending, sub.PendingBestEffortTaskNum())
		assert.Equal(t, subReady+subPending >= sub.MinAvailable, sub.IsReady())
	}
}

func TestReadinessTaskTransitions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		minimum    int32
		bestEffort bool
	}{
		{"all required", 4, false}, {"elastic", 2, false}, {"best effort", 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := NewJobInfo("ns/job")
			job.MinAvailable = tc.minimum
			job.TaskMinAvailable = map[string]int32{"worker": 2, "master": 2}
			job.TaskMinAvailableTotal = 4
			checkReadinessAgainstTasks(t, job) // Also initialize checks before tasks arrive.
			for i := 0; i < 4; i++ {
				resources := BuildResourceList("1", "1Gi")
				if tc.bestEffort && i%2 == 0 {
					resources = v1.ResourceList{}
				}
				pod := buildPod("ns", fmt.Sprint(i), "", v1.PodPending, resources, nil, nil)
				pod.Annotations = map[string]string{"volcano.sh/task-spec": []string{"worker", "master"}[i%2]}
				task := NewTaskInfo(pod)
				task.Job = job.UID
				job.AddTaskInfo(task)
				checkReadinessAgainstTasks(t, job)
			}
			for _, status := range []TaskStatus{Allocated, Pending, Pipelined, Pending, Allocated, Binding, Bound, Running, Releasing, Running, Succeeded, Failed, Pending} {
				for _, task := range job.Tasks {
					job.UpdateTaskStatus(task, status)
					checkReadinessAgainstTasks(t, job)
				}
			}
			clone := job.Clone()
			require.Len(t, clone.Tasks, 4)
			checkReadinessAgainstTasks(t, clone)
			for _, task := range clone.Tasks {
				clone.UpdateTaskStatus(task, Allocated)
				checkReadinessAgainstTasks(t, clone)
				checkReadinessAgainstTasks(t, job)
			}
			for _, task := range job.Tasks {
				job.DeleteTaskInfo(task)
				checkReadinessAgainstTasks(t, job)
			}
			checkReadinessAgainstTasks(t, clone)
		})
	}
}

func TestReadinessReplacementAndRegrouping(t *testing.T) {
	job := NewJobInfo("ns/job")
	pg := &PodGroup{PodGroup: scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "ns"},
		Spec:       scheduling.PodGroupSpec{MinMember: 2, MinTaskMember: map[string]int32{"worker": 1, "master": 1}},
	}}
	job.SetPodGroup(pg)
	for i := 0; i < 2; i++ {
		pod := buildPod("ns", fmt.Sprint(i), "", v1.PodPending, v1.ResourceList{}, nil, map[string]string{"partition": fmt.Sprint(i)})
		pod.Annotations = map[string]string{"volcano.sh/task-spec": "worker"}
		task := NewTaskInfo(pod)
		task.Job = job.UID
		job.AddTaskInfo(task)
	}
	checkReadinessAgainstTasks(t, job)
	// An informer update removes the old TaskInfo before adding its replacement.
	for _, old := range job.Tasks {
		pod := old.Pod.DeepCopy()
		pod.Annotations["volcano.sh/task-spec"] = "master"
		pod.Spec.Containers[0].Resources.Requests = BuildResourceList("1", "1Gi")
		replacement := NewTaskInfo(pod)
		replacement.Job = job.UID
		job.DeleteTaskInfo(old)
		job.AddTaskInfo(replacement)
		checkReadinessAgainstTasks(t, job)
		break
	}
	for _, grouped := range []bool{true, false} {
		updated := pg.Clone()
		if grouped {
			updated.Spec.SubGroupPolicy = []scheduling.SubGroupPolicySpec{{Name: "partition", MatchLabelKeys: []string{"partition"}, SubGroupSize: ptr.To(int32(1)), MinSubGroups: ptr.To(int32(2))}}
		}
		job.SetPodGroup(updated)
		checkReadinessAgainstTasks(t, job)
		for _, task := range job.Tasks {
			job.UpdateTaskStatus(task, Allocated)
			checkReadinessAgainstTasks(t, job)
			job.UpdateTaskStatus(task, Pending)
			checkReadinessAgainstTasks(t, job)
		}
		checkReadinessAgainstTasks(t, job.Clone())
	}
}

func BenchmarkGangReadiness(b *testing.B) {
	for _, n := range []int{1000, 2000, 4000, 8000} {
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				job := NewJobInfo("ns/job")
				job.MinAvailable = int32(n)
				job.TaskMinAvailable = map[string]int32{"worker": int32(n)}
				job.TaskMinAvailableTotal = int32(n)
				tasks := make([]*TaskInfo, n)
				for j := range tasks {
					pod := buildPod("ns", fmt.Sprint(j), "", v1.PodPending, BuildResourceList("1", "1Gi"), nil, nil)
					pod.Annotations = map[string]string{"volcano.sh/task-spec": "worker"}
					tasks[j] = NewTaskInfo(pod)
					tasks[j].Job = job.UID
					job.AddTaskInfo(tasks[j])
				}
				b.StartTimer()
				for j, task := range tasks {
					job.UpdateTaskStatus(task, Allocated)
					ready := job.CheckTaskReady() && job.CheckSubJobReady() && job.IsReady()
					if ready != (j == n-1) {
						b.Fatalf("unexpected readiness after %d allocations", j+1)
					}
				}
			}
		})
	}
}
