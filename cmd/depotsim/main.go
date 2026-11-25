// Command depotsim is a deterministic offline simulator of an overnight electric-bus depot. It compares
// uncoordinated charging with the allocation strategies of the engine package ("equal", "need", "weighted") on the
// same declared scenario, and writes indicators, time series and sensitivity tables.
//
//	go run ./cmd/depotsim -scenario cmd/depotsim/scenarios -out ../docs/results
//
// -scenario takes a JSON file or a directory (every *.json, in name order).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	scn := flag.String("scenario", "cmd/depotsim/scenarios", "scenario JSON file or directory of scenarios")
	out := flag.String("out", "../docs/results", "output directory")
	sens := flag.Bool("sensitivity", true, "run the sweeps declared in the scenarios")
	flag.Parse()

	if err := run(*scn, *out, *sens); err != nil {
		fmt.Fprintln(os.Stderr, "depotsim:", err)
		os.Exit(1)
	}
}

func run(scnPath, outDir string, sensitivity bool) error {
	files, err := scenarioFiles(scnPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(outDir, "timeseries"), 0o755); err != nil {
		return err
	}
	var runs []ScenarioRun
	var sens []SweepRow
	for _, f := range files {
		s, err := LoadScenario(f)
		if err != nil {
			return err
		}
		sr := ScenarioRun{Scenario: s, File: filepath.ToSlash(f), Results: map[string]Result{}}
		for _, st := range Strategies {
			r := Simulate(s, st)
			sr.Results[st] = r
			if err := writeSeries(filepath.Join(outDir, "timeseries", s.ID+"_"+st+".csv"), s, r); err != nil {
				return err
			}
		}
		runs = append(runs, sr)
		fmt.Printf("%-4s %s\n", s.ID, s.Title)
		if sensitivity && s.Sweeps != nil {
			rows, err := Sweep(s)
			if err != nil {
				return err
			}
			sens = append(sens, rows...)
		}
	}
	if err := writeSummary(filepath.Join(outDir, "summary.csv"), runs); err != nil {
		return err
	}
	if err := writeBuses(filepath.Join(outDir, "buses.csv"), runs); err != nil {
		return err
	}
	if err := writeSweeps(filepath.Join(outDir, "sensitivity.csv"), sens); err != nil {
		return err
	}
	readme := filepath.Join(outDir, "README.md")
	if err := os.WriteFile(readme, []byte(Report(runs, sens)), 0o644); err != nil {
		return err
	}
	fmt.Println("written:", readme)
	return nil
}

func scenarioFiles(p string) ([]string, error) {
	fi, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return []string{p}, nil
	}
	entries, err := os.ReadDir(p)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			files = append(files, filepath.Join(p, e.Name()))
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no *.json scenarios in %s", p)
	}
	return files, nil
}
