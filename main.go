package main

import (
	"github.com/c12s/celestial/model/config"
	"github.com/c12s/celestial/service"
	"github.com/c12s/celestial/storage/etcd"
	"log"
	"time"
)

func main() {
	// Run()
	// Load configurations
	conf, err := config.ConfigFile()
	if err != nil {
		log.Fatal(err)
	}

	//Load database
	db, err := etcd.New(conf, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	db.Init()

	//Start Server
	service.Run(db, conf)
}
