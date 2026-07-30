package heuristic

import "testing"

func TestParseSweepParams_Defaults(t *testing.T) {
	p := parseSweepParams(nil)
	if p.CompletenessMin != 0.9 || p.RecurrenceMin != 2 {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseSweepParams_PartialOverride(t *testing.T) {
	p := parseSweepParams([]byte(`{"completeness_min": 0.75}`))
	if p.CompletenessMin != 0.75 {
		t.Fatalf("expected completeness_min override to apply, got %v", p.CompletenessMin)
	}
	if p.RecurrenceMin != 2 {
		t.Fatalf("expected recurrence_min to keep its default, got %v", p.RecurrenceMin)
	}
}

func TestParseSweepParams_MalformedFallsBackToDefaults(t *testing.T) {
	p := parseSweepParams([]byte(`not json`))
	if p.CompletenessMin != 0.9 || p.RecurrenceMin != 2 {
		t.Fatalf("expected malformed params to fall back to defaults, got %+v", p)
	}
}
