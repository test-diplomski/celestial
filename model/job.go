package model

type Job struct {
	Labels  KVS `json:"labels"`
	Configs KVS `json:"configs"`
	Secrets KVS `json:"secrets"`
}

// Test is specified labels are present in job
func (self *Job) testLabels(labels KVS) bool {
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
func (self *Job) AddConfig(labels, data KVS, kind int) {
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
