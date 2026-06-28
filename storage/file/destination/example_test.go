package destination_test

import (
	"bufio"
	"context"
	"fmt"
	"os"

	filedest "github.com/auho/go-toolkit-flow/v3/storage/file/destination"
)

// ExampleNewLine demonstrates a file destination that writes string items as
// lines to a file. A temporary file is used so the example is self-contained.
func ExampleNewLine() {
	// Create a temporary file for the destination to write to.
	f, err := os.CreateTemp("", "example-dest-*")
	if err != nil {
		fmt.Println("create temp error:", err)
		return
	}
	name := f.Name()
	defer os.Remove(name)
	f.Close()

	d, err := filedest.NewLine(filedest.Config{Name: name})
	if err != nil {
		fmt.Println("new error:", err)
		return
	}

	ctx := context.Background()
	if err := d.Prepare(ctx); err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	d.Accept()
	_ = d.Receive([]string{"line1", "line2", "line3"})
	d.Done()

	if err := d.Finish(); err != nil {
		fmt.Println("finish error:", err)
		return
	}
	defer d.Close()

	// Read back the file and count lines.
	file, err := os.Open(name)
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer file.Close()

	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}

	fmt.Println("lines:", count)
	// Output:
	// lines: 3
}
