package main

import (
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/rest"
	"github.com/ethanrowe/botlnek/pkg/store/inmemory"
	"net/http"
	"os"
)

func main() {
	mux := http.NewServeMux()

	store := inmemory.NewInMemoryStore()
	app := &rest.RestApplication{
		DomainWriter:    store,
		DomainReader:    store,
		PartitionWriter: store,
		PartitionReader: store,
	}
	app.ApplyRoutes(mux)

	server := &http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}

	fmt.Println("Launching server on", server.Addr)
	err := server.ListenAndServe()
	if err != nil {
		fmt.Println("Error running server:", err.Error())
		os.Exit(255)
	}
	fmt.Println("Done serving.")
}
