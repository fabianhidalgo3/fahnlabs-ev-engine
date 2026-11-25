package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/fhidalgo/emob/motor/internal/engine"
)

type engineWeights = engine.Weights

func f1(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
func f2(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
func f3(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }

func writeCSV(path string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.WriteAll(rows); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

var metricHeader = []string{"peak_kw", "peak15_kw", "peak_ev_kw", "peak_reduction_pct", "peak15_reduction_pct",
	"departures", "met_min_soc", "met_min_soc_pct", "met_target_pct", "min_departure_soc", "mean_departure_soc",
	"energy_kwh", "battery_kwh", "mean_connection_h", "mean_charging_h", "charger_utilization",
	"setpoint_changes", "limit_violations", "contingency_bus_intervals", "contingency_buses", "never_charged",
	"avoided_peak15_kw", "avoided_demand_clp_month", "avoided_upgrade_kw", "avoided_upgrade_clp"}

// avoided returns the avoided 15-min peak, its monthly demand charge, the avoided connection enlargement (kW) and
// its investment, against the uncoordinated baseline (placeholder tariffs).
func avoided(s *Scenario, m, base Metrics) (kw, clpMonth, upKW, upCLP float64) {
	if m.Strategy == Uncoordinated {
		return 0, 0, 0, 0
	}
	kw = base.Peak15KW - m.Peak15KW
	clpMonth = kw * s.DemandChargeCLPPerKWMonth
	if m.PeakKW <= s.SiteLimitKW+1e-6 {
		upKW = max(0, base.PeakKW-s.SiteLimitKW)
	}
	return kw, clpMonth, upKW, upKW * s.UpgradeCLPPerKW
}

func metricCells(s *Scenario, m, base Metrics) []string {
	a, b, c, d := avoided(s, m, base)
	return []string{f1(m.PeakKW), f1(m.Peak15KW), f1(m.PeakEVKW), f1(reduction(base.PeakKW, m.PeakKW)),
		f1(reduction(base.Peak15KW, m.Peak15KW)), strconv.Itoa(m.Departures), strconv.Itoa(m.MetMin),
		f1(pct(m.MetMin, m.Departures)), f1(pct(m.MetTarget, m.Departures)), f1(m.MinDepSoC), f1(m.MeanDepSoC),
		f1(m.EnergyKWh), f1(m.BatteryKWh), f2(m.MeanConnectH), f2(m.MeanChargingH), f3(m.Utilization),
		strconv.Itoa(m.SetpointChg), strconv.Itoa(m.LimitViol), strconv.Itoa(m.ContingencyBI),
		strconv.Itoa(m.ContingencyB), strconv.Itoa(m.NeverCharged), f1(a), fmt.Sprintf("%.0f", b), f1(c),
		fmt.Sprintf("%.0f", d)}
}

func writeSummary(path string, runs []ScenarioRun) error {
	rows := [][]string{append([]string{"scenario", "strategy", "site_limit_kw", "interval_min"}, metricHeader...)}
	for _, r := range runs {
		base := r.Results[Uncoordinated].Metrics
		for _, st := range Strategies {
			m := r.Results[st].Metrics
			rows = append(rows, append([]string{r.Scenario.ID, st, f1(r.Scenario.SiteLimitKW),
				strconv.Itoa(r.Scenario.IntervalMin)}, metricCells(r.Scenario, m, base)...))
		}
	}
	return writeCSV(path, rows)
}

func writeBuses(path string, runs []ScenarioRun) error {
	rows := [][]string{{"scenario", "strategy", "bus", "arrival_soc", "departure_soc", "min_soc", "target_soc",
		"energy_kwh", "contingency_intervals"}}
	for _, r := range runs {
		for _, st := range Strategies {
			for _, b := range r.Results[st].Metrics.PerBus {
				rows = append(rows, []string{r.Scenario.ID, st, b.ID, f1(b.ArrivalSoC), f2(b.DepSoC), f1(b.MinSoC),
					f1(b.TargetSoC), f1(b.EnergyKWh), strconv.Itoa(b.Contingency)})
			}
		}
	}
	return writeCSV(path, rows)
}

func writeSweeps(path string, sweeps []SweepRow) error {
	rows := [][]string{append([]string{"sweep", "scenario", "value", "strategy"}, metricHeader...)}
	for _, r := range sweeps {
		rows = append(rows, append([]string{r.Sweep, r.Scenario, r.Value, r.Strategy}, metricCells(r.V, r.M, r.BaseM)...))
	}
	return writeCSV(path, rows)
}

// writeSeries writes one row per control interval: site totals, then power and SoC of each bus.
func writeSeries(path string, s *Scenario, r Result) error {
	head := []string{"minute", "clock", "aux_max_kw", "available_kw", "assigned_kw", "site_avg_kw", "site_max_kw", "limit_kw"}
	for _, b := range s.Buses {
		head = append(head, b.ID+"_kw")
	}
	for _, b := range s.Buses {
		head = append(head, b.ID+"_soc")
	}
	rows := [][]string{head}
	for _, it := range r.Series {
		row := []string{strconv.Itoa(it.Min), s.clock(it.Min), f1(it.AuxMaxKW), f1(it.AvailKW), f1(it.AssignedKW),
			f1(it.SiteAvgKW), f1(it.SiteMaxKW), f1(s.SiteLimitKW)}
		for _, v := range it.BusKW {
			row = append(row, f1(v))
		}
		for _, v := range it.BusSoC {
			row = append(row, f2(v))
		}
		rows = append(rows, row)
	}
	return writeCSV(path, rows)
}
