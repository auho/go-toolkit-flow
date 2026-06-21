package testutil

import (
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

// LoadEnv loads .env.test into environment variables.
// It searches for .env.test upward from the current working directory until found or reaching the filesystem root.
// On missing or load failure it only logs a warning (non-fatal) — existing env vars still apply (CI-friendly).
// Existing env vars take precedence and are not overwritten by the file.
func LoadEnv() {
	path, err := findEnvFileUpward(".env.test")
	if err != nil {
		log.Println("testutil: .env.test not found, falling back to existing env vars:", err)
		return
	}
	if err := godotenv.Load(path); err != nil {
		log.Println("testutil: .env.test load failed, falling back to existing env vars:", err)
	}
}

// findEnvFileUpward searches for the target file upward from the current working directory,
// returning the first found absolute path.
func findEnvFileUpward(name string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		p := filepath.Join(cwd, name)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
		parent := filepath.Dir(cwd)
		if parent == cwd {
			break
		}
		cwd = parent
	}
	return "", errors.New(".env.test not found in any parent directory")
}
