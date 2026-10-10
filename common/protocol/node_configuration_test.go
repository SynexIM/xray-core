package protocol

import "testing"

func TestNodeConfigurationIsCopiedAndRetainsZeroFields(t *testing.T) {
	s := &NodeFairScheduler{}
	s.SetClassPolicies([]*ClassPolicy{{Name: "z", Weight: 2}, {Name: "a", FloorBytePerSec: 125000}})
	s.SetCongestionHysteresis(85, 70, 5)
	config := s.Configuration()
	if config.EnterPercent != 85 || config.ExitPercent != 70 || config.ExitTicks != 5 || len(config.Classes) != 2 || config.Classes[0].Name != "a" || config.Classes[0].Weight != 0 {
		t.Fatal("configuration snapshot lost fields")
	}
	config.Classes[0].FloorBytePerSec = 0
	if s.ClassPolicyFor("a").FloorBytePerSec != 125000 {
		t.Fatal("snapshot mutation changed scheduler")
	}
}
