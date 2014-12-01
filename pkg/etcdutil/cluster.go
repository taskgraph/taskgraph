/*
   Copyright 2014 CoreOS, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package etcdutil

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/etcdhttp"
	"github.com/coreos/etcd/pkg/types"
)

const (
	tickDuration = 10 * time.Millisecond
	clusterName  = "etcd"
)

func newLocalListener(t *testing.T) net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

type member struct {
	etcdserver.ServerConfig
	PeerListeners, ClientListeners []net.Listener

	s   *etcdserver.EtcdServer
	hss []*httptest.Server
}

func StartNewEtcdServer(t *testing.T, name string) *member {
	m := MustNewMember(t, name)
	m.Launch()
	return m
}

func (m *member) URL() string {
	return fmt.Sprintf("http://%s", m.ClientListeners[0].Addr().String())
}

func MustNewMember(t *testing.T, name string) *member {
	var err error
	m := &member{}

	pln := newLocalListener(t)
	m.PeerListeners = []net.Listener{pln}
	m.PeerURLs, err = types.NewURLs([]string{"http://" + pln.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}

	cln := newLocalListener(t)
	m.ClientListeners = []net.Listener{cln}
	m.ClientURLs, err = types.NewURLs([]string{"http://" + cln.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}

	m.Name = name

	m.DataDir, err = ioutil.TempDir(os.TempDir(), "etcd")
	if err != nil {
		t.Fatal(err)
	}
	clusterStr := fmt.Sprintf("%s=http://%s", name, pln.Addr().String())
	m.Cluster, err = etcdserver.NewClusterFromString(clusterName, clusterStr)
	if err != nil {
		t.Fatal(err)
	}
	m.NewCluster = true
	m.Transport = newTransport()
	return m
}

// Launch starts a member based on ServerConfig, PeerListeners
// and ClientListeners.
func (m *member) Launch() error {
	var err error
	if m.s, err = etcdserver.NewServer(&m.ServerConfig); err != nil {
		return fmt.Errorf("failed to initialize the etcd server: %v", err)
	}
	m.s.Ticker = time.Tick(tickDuration)
	m.s.SyncTicker = time.Tick(500 * time.Millisecond)
	m.s.Start()

	for _, ln := range m.PeerListeners {
		hs := &httptest.Server{
			Listener: ln,
			Config:   &http.Server{Handler: etcdhttp.NewPeerHandler(m.s)},
		}
		hs.Start()
		m.hss = append(m.hss, hs)
	}
	for _, ln := range m.ClientListeners {
		hs := &httptest.Server{
			Listener: ln,
			Config:   &http.Server{Handler: etcdhttp.NewClientHandler(m.s)},
		}
		hs.Start()
		m.hss = append(m.hss, hs)
	}
	return nil
}

// Terminate stops the member and removes the data dir.
func (m *member) Terminate(t *testing.T) {
	m.s.Stop()
	for _, hs := range m.hss {
		hs.CloseClientConnections()
		hs.Close()
	}
	if err := os.RemoveAll(m.ServerConfig.DataDir); err != nil {
		t.Fatal(err)
	}
}

func newTransport() *http.Transport {
	tr := &http.Transport{}
	// TODO: need the support of graceful stop in Sender to remove this
	tr.DisableKeepAlives = true
	tr.Dial = (&net.Dialer{Timeout: 100 * time.Millisecond}).Dial
	return tr
}
