package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cheggaaa/pb"
	"github.com/dustin/go-humanize"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type Country struct {
	Id             int
	Name           string
	UserSlabsMem   uint64
	PhotoSlabsMem  uint64
	TotalMem       uint64
	TotalMemWillBe uint64
}

type Server struct {
	Hostname              string
	Countries             []Country
	TotalMem              uint64
	AllCountriesMem       uint64
	AllCountriesMemWillBe uint64
}

type Meetmakers struct {
	Servers []Server
}

type CountryT struct {
	Id   int
	Name string
}

type CountryConfT struct {
	Country []CountryT `json: country`
}

func getMeetmakersMemStat(meetmakers *Meetmakers) error {
	type rs struct {
		i          int
		j          int
		userSlabs  uint64
		photoSlabs uint64
		err        error
	}

	totalCountries := 0

	for i, _ := range meetmakers.Servers {
		for range meetmakers.Servers[i].Countries {
			totalCountries += 1
		}
	}

	log.Println("Getting memory stats from meetmaker daemons...")
	progressBar := pb.New(totalCountries * 2)
	progressBar.ShowTimeLeft = false
	progressBar.ShowCounters = false
	progressBar.SetWidth(80)
	progressBar.Start()

	c := make(chan rs)

	for i, _ := range meetmakers.Servers {
		for j, _ := range meetmakers.Servers[i].Countries {
			addr := fmt.Sprintf("%v:%v", meetmakers.Servers[i].Hostname, 13000+meetmakers.Servers[i].Countries[j].Id)

			go func(addr string, i int, j int) {
				userSlabs, photoSlabs, err := getMeetmakerMemStat(addr)
				c <- rs{
					i:          i,
					j:          j,
					userSlabs:  userSlabs,
					photoSlabs: photoSlabs,
					err:        err,
				}
				return
			}(addr, i, j)

			progressBar.Increment()
		}
	}

	for i, _ := range meetmakers.Servers {
		for range meetmakers.Servers[i].Countries {
			res := <-c
			if res.err != nil {
				return res.err
			}
			progressBar.Increment()
			meetmakers.Servers[res.i].Countries[res.j].UserSlabsMem = res.userSlabs
			meetmakers.Servers[res.i].Countries[res.j].PhotoSlabsMem = res.photoSlabs
		}
	}

	progressBar.Finish()

	return nil
}

func getMeetmakerMemStat(addr string) (uint64, uint64, error) {

	result := struct {
		UserSlabs  uint64 `json:"user_slabs"`
		PhotoSlabs uint64 `json:"photo_slabs"`
	}{}

	conn, err := net.DialTimeout("tcp", addr, time.Second*5)
	if err != nil {
		return 0, 0, err
	}

	conn.SetDeadline(time.Now().Add(time.Second * 5))
	fmt.Fprintf(conn, "service_stats_mem_usage {}\n")

	bytes, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return 0, 0, err
	}

	if err = json.Unmarshal(bytes[len("service_stats_mem_usage "):], &result); err != nil {
		return 0, 0, err
	}

	return result.UserSlabs, result.PhotoSlabs, nil
}

func getCountryConf(server string, config *ssh.ClientConfig) (CountryConfT, error) {
	var result CountryConfT

	sshConn, err := ssh.Dial("tcp", fmt.Sprintf("%v:22", server), config)
	if err != nil {
		return result, err
	}

	defer sshConn.Close()

	sshSession, err := sshConn.NewSession()
	if err != nil {
		return result, err
	}

	defer sshSession.Close()

	sshOutput, err := sshSession.Output("cat /local/meetmaker/conf/country.conf")
	if err != nil {
		return result, err
	}

	if err = json.Unmarshal(sshOutput, &result); err != nil {
		return result, nil
	}

	return result, nil
}

func getServersMemory(meetmakers *Meetmakers, sshConfig *ssh.ClientConfig) error {

	type rs struct {
		i      int
		server Server
		err    error
	}

	var (
		err error
		c   chan rs = make(chan rs)
	)

	log.Println("Getting memory stats from servers via ssh...")
	progressBar := pb.New(len(meetmakers.Servers) * 2)
	progressBar.ShowTimeLeft = false
	progressBar.ShowCounters = false
	progressBar.SetWidth(80)
	progressBar.Start()

	for i, s := range meetmakers.Servers {

		go func(i int, server Server) {
			err := getServerAndCountriesTotalMemory(&server, sshConfig)
			c <- rs{i, server, err}
		}(i, s)

		progressBar.Increment()

	}

	for range meetmakers.Servers {
		res := <-c
		if res.err != nil {
			err = res.err
			progressBar.Increment()
			continue
		}
		meetmakers.Servers[res.i] = res.server
		progressBar.Increment()
	}

	if err != nil {
		return err
	}

	progressBar.Finish()

	return nil
}

func getServerAndCountriesTotalMemory(server *Server, config *ssh.ClientConfig) error {
	var cmd string

	sshConn, err := ssh.Dial("tcp", fmt.Sprintf("%v:22", server.Hostname), config)
	if err != nil {
		return err
	}

	defer sshConn.Close()

	sshSession, err := sshConn.NewSession()
	if err != nil {
		return err
	}
	defer sshSession.Close()

	cmd = "cat /proc/meminfo | grep MemTotal | awk '{print $2}'"
	resBytes, err := sshSession.Output(cmd)
	if err != nil {
		return fmt.Errorf("error while executing '%v' on server: %v", cmd, err)
	}

	resString := string(resBytes)
	resString = strings.TrimFunc(resString, unicode.IsSpace)

	mem, err := strconv.Atoi(resString)
	if err != nil {
		return err
	}
	server.TotalMem = uint64(mem) * 1024

	for i, _ := range server.Countries {

		sshSession, err := sshConn.NewSession()
		if err != nil {
			return err
		}
		defer sshSession.Close()

		cmd = fmt.Sprintf("cat \"/local/meetmaker/var/meetmaker-%v.pid\"", server.Countries[i].Name)
		resBytes, err := sshSession.Output(cmd)
		if err != nil {
			return fmt.Errorf("error while executing '%v' on server '%v': %v", cmd, server.Hostname, err)
		}

		resString := string(resBytes)
		resString = strings.TrimFunc(resString, unicode.IsSpace)

		pid, err := strconv.Atoi(resString)
		if err != nil {
			return err
		}

		sshSession, err = sshConn.NewSession()
		if err != nil {
			return err
		}
		defer sshSession.Close()

		cmd = fmt.Sprintf("ps --no-headers -Fs %v | awk '{sum += $6} END {print sum}'", pid)
		resBytes, err = sshSession.Output(cmd)
		if err != nil {
			return fmt.Errorf("error while executing '%v' on server '%v': %v", cmd, server.Hostname, err)
		}

		resString = string(resBytes)
		resString = strings.TrimFunc(resString, unicode.IsSpace)

		mem, err := strconv.Atoi(resString)
		if err != nil {
			return err
		}

		server.Countries[i].TotalMem = uint64(mem) * 1024
	}

	return nil
}

func getMeetmakers(sshConfig *ssh.ClientConfig) (Meetmakers, error) {

	var (
		meetmakers Meetmakers
		type_      string
		nodes      string
		platform   string
	)

	db, err := sql.Open("mysql", "bdsm_ro@tcp(bdsm.mlan:3306)/PuppetVirtUsers")
	if err != nil {
		return meetmakers, err
	}

	defer db.Close()

	if err = db.Ping(); err != nil {
		return meetmakers, err
	}

	rows, err := db.Query("select type, nodes, platform from badoo_services_map where service = \"meetmaker\"")
	if err != nil {
		return meetmakers, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&type_, &nodes, &platform)
		if err != nil {
			return meetmakers, err
		}

		if strings.Index(nodes, ",") != -1 {
			log.Printf("servers %v are in migration process right now, skipping...", nodes)
			continue
		}

		hostname := fmt.Sprintf("%v.%v", nodes, platform)

		found := false
		for i, _ := range meetmakers.Servers {
			if meetmakers.Servers[i].Hostname == hostname {
				meetmakers.Servers[i].Countries = append(meetmakers.Servers[i].Countries, Country{
					Name: type_,
				})
				found = true
				break
			}
		}

		if found == false {
			server := Server{
				Hostname: hostname,
				Countries: []Country{
					Country{
						Name: type_,
					},
				},
			}
			meetmakers.Servers = append(meetmakers.Servers, server)
		}
	}

	err = rows.Err()
	if err != nil {
		return meetmakers, err
	}

	countryConf, err := getCountryConf(meetmakers.Servers[0].Hostname, sshConfig)
	if err != nil {
		return meetmakers, err
	}

	for i, _ := range meetmakers.Servers {
		for j, _ := range meetmakers.Servers[i].Countries {
			found := false
			for _, c := range countryConf.Country {
				if c.Name == meetmakers.Servers[i].Countries[j].Name {
					meetmakers.Servers[i].Countries[j].Id = c.Id
					found = true
					break
				}
			}
			if found == false {
				return meetmakers, fmt.Errorf("could not find country '%s' in country.conf",
					meetmakers.Servers[i].Countries[j].Name)
			}
		}
	}

	if err := getMeetmakersMemStat(&meetmakers); err != nil {
		return meetmakers, err
	}

	if err := getServersMemory(&meetmakers, sshConfig); err != nil {
		return meetmakers, err
	}

	return meetmakers, nil
}

func main() {
	var shards = flag.Int("shards", 4, "shard count")
	var votes_percent = flag.Float64("votes_percent", 0.42, "votes percent")
	flag.Parse()

	sshConfig := ssh.ClientConfig{
		User: os.Getenv("LOGNAME"),
	}

	if sshAgentConn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		sshAgent := agent.NewClient(sshAgentConn)
		sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeysCallback(sshAgent.Signers))
	}

	meetmakers, err := getMeetmakers(&sshConfig)
	if err != nil {
		log.Fatal(err)
	}

	for i, server := range meetmakers.Servers {
		serverMemWillBe := uint64(0)
		serverMemWas := uint64(0)
		for j, country := range server.Countries {
			VoteSlabsMem := country.TotalMem - country.PhotoSlabsMem - country.UserSlabsMem
			countryTotalMemWillBe := (country.UserSlabsMem + country.PhotoSlabsMem + uint64(float64(VoteSlabsMem)**votes_percent)) * uint64(*shards)
			serverMemWas += country.TotalMem
			serverMemWillBe += countryTotalMemWillBe
			meetmakers.Servers[i].Countries[j].TotalMemWillBe = countryTotalMemWillBe
		}
		meetmakers.Servers[i].AllCountriesMemWillBe = serverMemWillBe
		meetmakers.Servers[i].AllCountriesMem = serverMemWas
	}

	for _, server := range meetmakers.Servers {

		fmt.Printf("%v: %v -> %v (which is %.2f%% of total memory on server)\n",
			server.Hostname,
			humanize.Bytes(server.AllCountriesMem),
			humanize.Bytes(server.AllCountriesMemWillBe),
			((float64(server.AllCountriesMemWillBe) / float64(server.TotalMem)) * 100.0))

		for _, country := range server.Countries {
			fmt.Printf("\t%v: %v -> %v\n",
				country.Name,
				humanize.Bytes(country.TotalMem),
				humanize.Bytes(country.TotalMemWillBe))
		}
	}

}
