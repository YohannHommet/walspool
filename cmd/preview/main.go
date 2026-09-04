package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	docsDir := filepath.Join(".", "docs")
	if _, err := os.Stat(filepath.Join(docsDir, "index.html")); err != nil {
		docsDir = filepath.Join("..", "docs")
	}

	fs := http.FileServer(http.Dir(docsDir))
	http.Handle("/", fs)

	port := "8088"
	fmt.Printf("🌐 walspool docs & landing page serving at: http://localhost:%s\n", port)
	fmt.Printf("📄 Serving directory: %s\n", docsDir)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}
