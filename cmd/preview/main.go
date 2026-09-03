package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	landingDir := filepath.Join(".", "landing")
	if _, err := os.Stat(filepath.Join(landingDir, "index.html")); err != nil {
		landingDir = filepath.Join("..", "landing")
	}

	fs := http.FileServer(http.Dir(landingDir))
	http.Handle("/", fs)

	port := "8088"
	fmt.Printf("🌐 walspool landing page serving at: http://localhost:%s\n", port)
	fmt.Printf("📄 Serving directory: %s\n", landingDir)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}
