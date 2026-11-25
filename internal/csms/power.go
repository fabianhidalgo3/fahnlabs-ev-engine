package csms

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/fhidalgo/emob/motor/internal/engine"
)

type budgetSession struct {
	engine.Session
	ch   *Charger
	k    *Connector
	maxW float64
}

// Budget is the power available for charging and the sessions competing for it.
type Budget struct {
	BuildingW, ReservedW, MarginW, AvailableW, MinW float64
	sessions                                        []budgetSession
}

// computeBudget builds the allocation input (caller holds the lock).
func (c *Csms) computeBudget() Budget {
	b := Budget{BuildingW: c.auxW, MinW: c.cfg.MinPowerKw * 1000}
	now := time.Now()
	for _, ch := range c.chargers {
		for _, k := range ch.Connectors {
			if k.TransactionID == 0 {
				continue
			}
			if !ch.Connected {
				// No communication: the last setpoint sent stays reserved (safety criterion).
				if k.LastSentW != nil {
					b.ReservedW += *k.LastSentW
				} else {
					b.ReservedW += k.AllocatedW
				}
				continue
			}
			v, plan := k.Vehicle, k.Plan
			dc := ch.CurrentType == "DC"
			// With an identified vehicle its own limit is known: on-board charger (AC) or DC acceptance (DC).
			// A charging plan may also set its own kW limit.
			maxW := ch.MaxPowerKw * 1000
			if v != nil {
				if !dc {
					maxW = math.Min(maxW, v.MaxACKW*1000)
				} else if v.MaxDCKW != nil {
					maxW = math.Min(maxW, *v.MaxDCKW*1000)
				}
			}
			if plan != nil && plan.MaxKW != nil {
				maxW = math.Min(maxW, *plan.MaxKW*1000)
			}
			// AC: 6 A (IEC 61851) on the phases used by the EV, rounded up to 100 W (setpoint granularity).
			// DC: no such minimum (the DC charger regulates power continuously), so a session may be paused at 0 W.
			sessMin := b.MinW
			if dc {
				sessMin = 0
			} else if v != nil {
				sessMin = math.Min(b.MinW, math.Ceil(6*c.cfg.Voltage*float64(min(v.Phases, c.cfg.Phases))/100)*100)
			}
			capW := maxW
			// Adaptation to the real demand (with hysteresis). Only judged with measurements taken once the EV has
			// settled on the last setpoint (otherwise an EV still ramping up would be cut).
			running := k.StartedAt > 0 && now.UnixMilli()-k.StartedAt > 20000
			settled := !k.LastMeterAt.IsZero() && !k.LastSentAt.IsZero() && k.LastMeterAt.Sub(k.LastSentAt) > 10*time.Second
			if running && k.AllocatedW > 0 {
				prev := maxW
				if k.CapW != nil {
					prev = *k.CapW
				}
				switch {
				case !settled:
					capW = prev
				case k.PowerW < k.AllocatedW-1500:
					capW = k.PowerW + 1500
				case k.PowerW >= k.AllocatedW-500:
					if prev >= maxW {
						capW = maxW
					} else {
						capW = math.Max(k.AllocatedW+2000, k.AllocatedW*1.5)
					}
				default:
					capW = prev
				}
				capW = math.Min(maxW, math.Max(sessMin, capW))
			}
			var need *float64
			target := targetSoCOf(v, plan)
			if v != nil && k.SoC != nil && target != nil {
				n := math.Max(0, (*target-*k.SoC)/100*v.BatteryKWh)
				need = &n
			}
			var hours *float64
			if plan != nil && plan.DepartureAt != nil {
				h := float64(*plan.DepartureAt-now.UnixMilli()) / 3.6e6
				hours = &h
			}
			prio := ch.Priority
			if v != nil {
				prio = v.Priority
			}
			m := sessMin
			es := engine.Session{Key: fmt.Sprintf("%s#%d", ch.ID, k.ID), CapW: capW, Priority: prio, StartedAt: k.StartedAt, MinW: &m, NeedKWh: need, HoursLeft: hours,
				SoC: k.SoC, TargetSoC: target, MinSoC: minSoCOf(v, plan), RatedW: maxW}
			if v != nil && v.BatteryKWh > 0 {
				kwh := v.BatteryKWh
				es.BatteryKWh = &kwh
			}
			b.sessions = append(b.sessions, budgetSession{Session: es, ch: ch, k: k, maxW: maxW})
		}
	}
	// Safety margin: absorbs auxiliary-load variation and measurement delay between cycles.
	b.MarginW = c.cfg.SafetyMarginKw * 1000
	b.AvailableW = math.Max(0, c.cfg.SiteLimitKw*1000-b.BuildingW-b.ReservedW-b.MarginW)
	return b
}

// Rebalance schedules a reallocation after `delay` (debounced: a newer request replaces a pending one).
func (c *Csms) Rebalance(reason string, delay time.Duration) {
	c.mu.Lock()
	if c.rebalanceTimer != nil {
		c.rebalanceTimer.Stop()
	}
	c.rebalanceTimer = time.AfterFunc(delay, func() {
		select {
		case c.rebalanceCh <- reason:
		default: // one already queued: it will see the latest state
		}
	})
	c.mu.Unlock()
}

// rebalanceLoop runs one reallocation at a time; requests arriving meanwhile are coalesced into one more run.
func (c *Csms) rebalanceLoop() {
	for reason := range c.rebalanceCh {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.AddLog("error", fmt.Sprintf("rebalance panic: %v", r), "")
				}
			}()
			c.doRebalance(reason)
		}()
	}
}

type change struct {
	ch *Charger
	k  *Connector
	w  float64
}

func (c *Csms) doRebalance(reason string) {
	c.mu.Lock()
	b := c.computeBudget()
	c.auxAtAlloc = b.BuildingW
	sessions := make([]engine.Session, len(b.sessions))
	for i, s := range b.sessions {
		sessions[i] = s.Session
	}
	params := c.cfg.params()
	alloc := engine.AllocateWith(b.AvailableW, b.MinW, c.cfg.Strategy, params, sessions)
	var evals map[string]engine.Evaluation
	if c.cfg.Strategy == "weighted" {
		evals = engine.Evaluate(b.AvailableW, params, sessions)
	}
	var decreases, increases []change
	increasing := map[*Connector]bool{}
	for _, s := range b.sessions {
		w := alloc[s.Key]
		capW := s.CapW
		s.k.CapW = &capW
		s.k.DemandLimited = s.CapW < s.maxW
		s.k.AllocatedW = w
		s.k.NeedKWh = s.NeedKWh
		s.k.Eval = nil
		if ev, ok := evals[s.Key]; ok {
			s.k.Eval = &ev
		}
		// Decreases are always sent (even small ones): otherwise the EV would keep the previous, higher setpoint.
		// Increases only above 200 W, so the charger is not flooded with tiny changes.
		switch prev := s.k.LastSentW; {
		case prev == nil || w > *prev+199:
			increases = append(increases, change{s.ch, s.k, w})
			increasing[s.k] = true
		case w < *prev:
			decreases = append(decreases, change{s.ch, s.k, w})
		}
	}
	ramp := time.Duration(c.cfg.RampDelayMs) * time.Millisecond
	c.mu.Unlock()

	// Decreases go first; only once those EVs had time to reduce are the others increased,
	// so the total never exceeds the limit during the transition.
	if len(decreases) > 0 {
		var wg sync.WaitGroup
		for _, d := range decreases {
			wg.Add(1)
			go func(d change) {
				defer wg.Done()
				c.sendTxProfile(d.ch, d.k, d.w, reason)
			}(d)
		}
		wg.Wait()
		if len(increases) > 0 {
			time.Sleep(ramp)
		}
	}
	if len(increases) > 0 {
		// Re-check that the increases still fit with the current state (it may have changed while waiting)
		c.mu.Lock()
		now := c.computeBudget()
		committed := 0.0
		for _, s := range b.sessions {
			if !increasing[s.k] && s.k.LastSentW != nil {
				committed += *s.k.LastSentW
			}
		}
		room := now.AvailableW - committed
		for i := range increases {
			w := math.Max(0, math.Min(increases[i].k.AllocatedW, math.Floor(room/100)*100))
			room -= w
			increases[i].k.AllocatedW = w
			increases[i].w = w
		}
		c.mu.Unlock()
		for _, inc := range increases {
			c.sendTxProfile(inc.ch, inc.k, inc.w, reason)
		}
	}
	c.changed()
}

func (c *Csms) sendTxProfile(ch *Charger, k *Connector, limitW float64, reason string) {
	c.mu.Lock()
	conn := ch.conn
	if conn == nil || k.TransactionID == 0 {
		c.mu.Unlock()
		return
	}
	prev := k.LastSentW
	w := limitW
	k.LastSentW = &w
	payload := map[string]any{
		"connectorId": k.ID,
		"csChargingProfiles": map[string]any{
			"chargingProfileId": 100 + k.ID, "transactionId": k.TransactionID, "stackLevel": 1,
			"chargingProfilePurpose": "TxProfile", "chargingProfileKind": "Relative", "chargingSchedule": c.schedule(limitW),
		},
	}
	c.mu.Unlock()

	ctx, cancel := bg()
	defer cancel()
	res, err := conn.Call(ctx, "SetChargingProfile", payload)
	if err == nil {
		var r struct {
			Status string `json:"status"`
		}
		_ = json.Unmarshal(res, &r)
		if r.Status != "Accepted" {
			err = fmt.Errorf("answer %s", r.Status)
		}
	}
	if err != nil {
		c.mu.Lock()
		k.LastSentW = prev
		c.mu.Unlock()
		c.AddLog("error", "SetChargingProfile failed: "+err.Error(), ch.ID)
		return
	}
	c.mu.Lock()
	k.LastSentAt = time.Now()
	c.mu.Unlock()
	c.AddLog("power", fmt.Sprintf("Setpoint %.1f kW → connector %d (%s)", limitW/1000, k.ID, reason), ch.ID)
}
