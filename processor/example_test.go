package processor_test

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/processor"
)

func ExampleBaseProcessor() {
	var bp processor.BaseProcessor

	bp.AddStateLine("processing batch 1")
	bp.AddStateLine("items: 100")

	bp.Outputln("start processing")
	bp.Outputf("processed %d items", 100)

	bp.Logln("debug: initializing")

	for _, s := range bp.State() {
		fmt.Println(s)
	}
	for _, s := range bp.Output() {
		fmt.Println(s)
	}
	for _, s := range bp.Log() {
		fmt.Println(s)
	}
	// Output:
	// processing batch 1
	// items: 100
	// start processing
	// processed 100 items
	// debug: initializing
}