package model

type Node struct {
	Labels  KVS   `json:"labels"`
	Configs KVS   `json:"configs"`
	Secrets KVS   `json:"secrets"`
	Jobs    []Job `json:"jobs"`
}

// Test if specified labels are present in node
func (self *Node) TestLabels(labels KVS) bool {
	if len(self.Labels.Kvs) != len(labels.Kvs) {
		return false
	}

	for k, _ := range labels.Kvs {
		if _, ok := self.Labels.Kvs[k]; !ok {
			return false
		}
	}

	return true
}

// If labels are present, add new configs
func (self *Node) AddConfig(data KVS, kind int) {
	switch kind {
	case 1:
		for k, v := range data.Kvs {
			self.Secrets.Kvs[k] = v
		}
	default:
		for k, v := range data.Kvs {
			self.Configs.Kvs[k] = v
		}
	}
}

// Select Jobs that contains labels or key-value pairs specified by user
// Return Job chanel from witch jobs will arrive
func (self *Node) SelectJobs(selector KVS) []Job {
	jobs := []Job{}
	for _, job := range self.Jobs {
		if job.testLabels(selector) {
			jobs = append(jobs, job)
		}
	}

	return jobs
}
