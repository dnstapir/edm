package runner

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue/file"
	"github.com/fsnotify/fsnotify"
)

var (
	exitProcess                     = os.Exit
	notifyContext                   = signal.NotifyContext
	newFSWatcher                    = fsnotify.NewWatcher
	openPebble                      = pebble.Open
	listenNet                       = net.Listen
	listenTLS                       = tls.Listen
	newFrameStreamSockInputFromPath = dnstap.NewFrameStreamSockInputFromPath
	newFrameStreamSockInput         = dnstap.NewFrameStreamSockInput
	listenAndServeHTTP              = func(s *http.Server) error { return s.ListenAndServe() }
	newFileQueue                    = file.New
	newAutoPahoConnection           = autopaho.NewConnection

	// Filesystem operation seams. These wrap the matching os package
	// functions so tests can inject failures into the file writers,
	// renamers and directory scanners without contorting the real
	// filesystem.
	osCreate   = os.Create
	osRename   = os.Rename
	osRemove   = os.Remove
	osMkdirAll = os.MkdirAll
	osReadDir  = os.ReadDir
	osStat     = os.Stat

	now                     = time.Now
	sleep                   = time.Sleep
	configUpdateDebounce    = 100 * time.Millisecond
	fsEventDebounce         = 100 * time.Millisecond
	diskCleanerInterval     = time.Minute
	monitorChannelInterval  = time.Second
	histogramSenderInterval = 10 * time.Second
	histogramSenderBackoff  = 15 * time.Second
)
