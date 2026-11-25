package store

import (
	"encoding/json"
	"testing"
)

func TestVehicleInputMinSocAndDC(t *testing.T) {
	base := Vehicle{Name: "Bus", BatteryKWh: 350, MaxACKW: 11, Phases: 3, TargetSoC: 90, Priority: 5}
	var in VehicleInput
	if err := json.Unmarshal([]byte(`{"min_soc": 40, "max_dc_kw": 150}`), &in); err != nil {
		t.Fatal(err)
	}
	v, err := in.merge(base)
	if err != nil || v.MinSoC == nil || *v.MinSoC != 40 || v.MaxDCKW == nil || *v.MaxDCKW != 150 {
		t.Fatalf("set: %+v %v", v, err)
	}
	// absent keys keep the value, null clears it
	var keep, clear VehicleInput
	_ = json.Unmarshal([]byte(`{"name": "Bus 2"}`), &keep)
	_ = json.Unmarshal([]byte(`{"min_soc": null}`), &clear)
	if k, _ := keep.merge(v); k.MinSoC == nil || k.MaxDCKW == nil {
		t.Fatalf("keep: %+v", k)
	}
	if c, _ := clear.merge(v); c.MinSoC != nil || c.MaxDCKW == nil {
		t.Fatalf("clear: %+v", c)
	}
	for _, body := range []string{`{"min_soc": 95}`, `{"min_soc": -1}`, `{"max_dc_kw": 0.5}`, `{"max_dc_kw": 1001}`} {
		var bad VehicleInput
		_ = json.Unmarshal([]byte(body), &bad)
		if _, err := bad.merge(base); err == nil {
			t.Errorf("%s: expected a validation error", body)
		}
	}
}
