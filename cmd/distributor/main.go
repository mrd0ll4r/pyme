package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/pkg/errors"

	"github.com/mrd0ll4r/pyme"
	"github.com/mrd0ll4r/pyme/tasks"
	"github.com/mrd0ll4r/pyme/tasks/distributor"
)

var (
	config string
)

func init() {
	flag.StringVar(&config, "c", "/etc/distributor.yaml", "location of the configuration file")
}

func main() {
	flag.Parse()

	configFile, err := parseConfigFile(config)
	if err != nil {
		log.Fatal(err)
	}

	_, portString, err := net.SplitHostPort(configFile.MainConfigBlock.HTTPEndpoint)
	if err != nil {
		log.Fatal(errors.Wrap(err, "unable to determine port"))
	}

	port, err := strconv.Atoi(portString)
	if err != nil {
		log.Fatal(errors.Wrap(err, "unable to parse port"))
	}

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	var nodeID pyme.NodeID

	homeDir, err := pyme.GetHomeDir()
	if err != nil {
		log.Println("unable to determine home directory:", err)
		log.Println("generating new node ID...")
		nodeID = pyme.NewNodeID()
	} else {
		nodeID, err = pyme.LoadNodeIDFromFile(filepath.Join(homeDir, ".pymeNodeID"))
		if err != nil {
			log.Println("unable to load node ID:", err)
			log.Println("generating new node ID...")
			nodeID = pyme.NewNodeID()
			err = nodeID.SaveToFile(filepath.Join(homeDir, ".pymeNodeID"))
			if err != nil {
				log.Println("unable to save node ID:", err)
			}
		} else {
			log.Println("loaded node ID")
		}
	}

	log.Println("I am", nodeID)

	distributor := distributor.NewDistributorLogic(nodeID, configFile.MainConfigBlock.DistributorConfig)

	httpTaskServer := tasks.NewHTTPDistributorServer(distributor, "", configFile.MainConfigBlock.HTTPEndpoint, configFile.MainConfigBlock.HTTPPrintDebugLogs)

	go func() {
		log.Println("starting HTTP on " + configFile.MainConfigBlock.HTTPEndpoint + "...")
		err := httpTaskServer.ListenAndServe()
		if err != nil {
			log.Println("unable to run HTTP:", err)
		}
	}()

	if configFile.MainConfigBlock.UseMDNS {
		mdnsServer, err := tasks.PublishDistributor(uint16(port))
		if err != nil {
			log.Fatal(err)
		}
		defer mdnsServer.Shutdown()
	} else {
		log.Println("not using mDNS")
	}

	shutdown := make(chan os.Signal)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown

	log.Println("shutting down...")
	httpTaskServer.Stop()
}
