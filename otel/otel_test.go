package otel

import (
	"context"
	"testing"
)

func TestInitTracerDisabled(t *testing.T) {
	shutdown, err := InitTracer(Config{ServiceName: "test"})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if shutdown == nil {
		t.Fatal("nil shutdown")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if Tracer("unit-test") == nil {
		t.Fatal("nil tracer")
	}
}

func TestInitTracerWithoutEndpointIsDisabled(t *testing.T) {
	shutdown, err := InitTracer(Config{ServiceName: "test", Enabled: true})
	if err != nil || shutdown == nil {
		t.Fatalf("error=%v nil=%v", err, shutdown == nil)
	}
}
