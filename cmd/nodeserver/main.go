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
	"github.com/mrd0ll4r/pyme/tasks/calculator"
	"github.com/mrd0ll4r/pyme/tasks/nodeserver"
	"github.com/mrd0ll4r/pyme/tasks/rater"
)

var (
	config string
)

func init() {
	flag.StringVar(&config, "c", "/etc/nodeserver.yaml", "location of the configuration file")
}

func main() {
	flag.Parse()

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

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

	pymeClusterRater := rater.NewPYMEClusterRater(configFile.MainConfigBlock.PYMCClusterRaterConfig)
	localizationCalculator := calculator.NewLocalizationCalculator([]tasks.Rater{pymeClusterRater})

	distributors := make([]tasks.Endpoint, 0)
	for _, addr := range configFile.MainConfigBlock.ManualDistributors {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			log.Println(errors.Wrap(err, "unable to determine distributor IP/port"))
			continue
		}

		p, err := strconv.Atoi(port)
		if err != nil {
			log.Println(errors.Wrap(err, "unable to parse port"))
			continue
		}

		ips, err := net.LookupIP(host)
		if err != nil || len(ips) == 0 {
			log.Println("unable to resolve distributor")
			continue
		}
		distributors = append(distributors, tasks.Endpoint{IP: ips[0], Port: uint16(p)})
	}

	if configFile.MainConfigBlock.UseMDNS {
		endpoints, err := tasks.DiscoverDistributors()
		if err != nil {
			log.Println(errors.Wrap(err, "unable to look up distributors using mDNS"))
		} else {
			distributors = append(distributors, endpoints...)
		}
	} else {
		log.Println("not using mDNS")
	}

	// remove duplicates
	filteredEndpoints := make([]tasks.Endpoint, 0)
	for _, ep := range distributors {
		dup := false
		for _, other := range filteredEndpoints {
			if ep.IP.Equal(other.IP) {
				dup = true
				break
			}
		}
		if !dup {
			filteredEndpoints = append(filteredEndpoints, ep)
		}
	}

	if len(filteredEndpoints) == 0 {
		log.Fatal("have no distributors")
	}

	var announceIP net.IP

	if configFile.MainConfigBlock.AnnounceIP != "" {
		announceIP = net.ParseIP(configFile.MainConfigBlock.AnnounceIP)
		if len(announceIP) == 0 {
			log.Fatal("unable to parse announce IP")
		}
	} else {
		log.Println("guessing announce IP")
		localIPs, err := tasks.GetNonLoopbackIPs()
		if err != nil {
			log.Fatal(errors.Wrap(err, "unable to determine own IP"))
		}
		if len(localIPs) == 0 {
			log.Fatal("unable to determine at least one local IP")
		}
		if len(localIPs) > 1 {
			log.Printf("discovered %d local IPs, will use %s to announce", len(localIPs), localIPs[0].String())
		}
		announceIP = localIPs[0]
	}

	log.Printf("using %q as announce IP", announceIP.String())

	nodeServer, err := nodeserver.NewNodeServerLogic(
		nodeID,
		[]tasks.Calculator{localizationCalculator},
		filteredEndpoints,
		tasks.Endpoint{IP: announceIP, Port: uint16(port)},
		configFile.MainConfigBlock.NodeServerConfig)
	if err != nil {
		log.Fatal(err)
	}

	httpTaskServer := tasks.NewHTTPNodeServer(nodeServer, "", configFile.MainConfigBlock.HTTPEndpoint, configFile.MainConfigBlock.HTTPPrintDebugLogs)

	go func() {
		log.Printf("starting HTTP server on %s...", configFile.MainConfigBlock.HTTPEndpoint)
		err := httpTaskServer.ListenAndServe()
		if err != nil {
			log.Fatal(errors.Wrap(err, "unable to run HTTP server"))
		}
	}()

	shutdown := make(chan os.Signal)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown

	log.Println("shutting down...")
	httpTaskServer.Stop()
	nodeServer.Stop()
}
