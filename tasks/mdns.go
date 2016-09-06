package tasks

import (
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/hashicorp/mdns"
	"github.com/pkg/errors"
)

const mDNSServiceName = "_pyme-taskdist._tcp"

// PublishDistributor publishes a Distributor running on the local machine on
// the specified on mDNS.
func PublishDistributor(port uint16) (*mdns.Server, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, errors.Wrap(err, "unable to determine hostname")
	}

	ips, err := GetNonLoopbackIPs()
	if err != nil {
		return nil, errors.Wrap(err, "unable to determine IPs")
	}

	log.Println("mDNS: creating service for", ips)

	service, err := mdns.NewMDNSService(
		host,
		mDNSServiceName,
		"",
		"",
		int(port),
		ips,
		nil,
	)

	if err != nil {
		return nil, errors.Wrap(err, "unable to create mDNS service")
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create mDNS server")
	}

	return server, nil
}

// GetNonLoopbackIPs returns a list of IP addresses of the local machine that
// are up and not loopback interfaces.
func GetNonLoopbackIPs() ([]net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	var ips []net.IP

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			// interface down
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			// loopback interface
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			ips = append(ips, ip)
		}
	}

	return ips, nil
}

// DiscoverDistributors tries to discover Distributors via mDNS.
func DiscoverDistributors() ([]Endpoint, error) {
	entries := make(chan *mdns.ServiceEntry, 4)
	var endpoints []Endpoint

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for entry := range entries {
			if strings.Contains(entry.Name, mDNSServiceName) {
				ep := Endpoint{Port: uint16(entry.Port)}
				if entry.AddrV4 != nil {
					ep.IP = entry.AddrV4
					endpoints = append(endpoints, ep)
					continue
				}
				if entry.AddrV6 != nil {
					ep.IP = entry.AddrV6
					endpoints = append(endpoints, ep)
					continue
				}
				log.Println("mdns: unable to determine IP for entry", entry)
			}
		}
	}()

	qp := mdns.DefaultParams(mDNSServiceName)
	qp.Entries = entries
	qp.WantUnicastResponse = true
	err := mdns.Query(qp)
	if err != nil {
		return nil, errors.Wrap(err, "unable to perform mDNS query")
	}
	close(entries)

	wg.Wait()
	return endpoints, nil
}
