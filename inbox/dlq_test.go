package inbox

import "testing"

func TestKafkaDLQValidation(t *testing.T) {
	if _, err := NewDLQProducer(nil, "dlq"); err == nil {
		t.Fatal("want error on nil producer")
	}
}
