package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"

	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/app"
	met "github.com/grafana/grafana/pkg/metric"
	"github.com/grafana/grafana/pkg/metric/helper"
	"github.com/raintank/raintank-metric/instrumented_nsq"

	"github.com/raintank/raintank-metric/eventdef"
	"github.com/raintank/raintank-metric/setting"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic         = flag.String("topic", "probe_events", "NSQ topic")
	channel       = flag.String("channel", "elasticsearch", "NSQ channel")
	maxInFlight   = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	totalMessages = flag.Int("n", 0, "total messages to process (will wait if starved)")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address (default: localhost:8125)")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog (default: standard)")

	consumerOpts     = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}

	eventsToEsOK   met.Count
	eventsToEsFail met.Count
	messagesSize   met.Meter
	msgsAge        met.Meter // in ms
	esPutDuration  met.Timer
	msgsHandleOK   met.Count
	msgsHandleFail met.Count
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type ESHandler struct {
	totalMessages int
	messagesDone  int
}

func NewESHandler(totalMessages int) (*ESHandler, error) {

	err := eventdef.InitElasticsearch()
	if err != nil {
		return nil, err
	}

	return &ESHandler{
		totalMessages: totalMessages,
	}, nil
}

func (k *ESHandler) HandleMessage(m *nsq.Message) error {
	log.Printf("received message.")
	k.messagesDone++
	format := "unknown"
	if m.Body[0] == '\x00' {
		format = "msgFormatJson"
	}
	var id int64
	buf := bytes.NewReader(m.Body[1:9])
	binary.Read(buf, binary.BigEndian, &id)
	produced := time.Unix(0, id)

	msgsAge.Value(time.Now().Sub(produced).Nanoseconds() / 1000)
	messagesSize.Value(int64(len(m.Body)))

	event := new(eventdef.EventDefinition)
	if err := json.Unmarshal(m.Body[9:], &event); err != nil {
		log.Printf("ERROR: failure to unmarshal message body via format %s: %s. skipping message", format, err)
		return nil
	}
	done := make(chan error, 1)
	go func() {
		pre := time.Now()
		if err := event.Save(); err != nil {
			fmt.Printf("ERROR: couldn't process %s: %s\n", event.Id, err)
			eventsToEsFail.Inc(1)
			done <- err
			return
		}
		esPutDuration.Value(time.Now().Sub(pre))
		eventsToEsOK.Inc(1)
		done <- nil
	}()

	if err := <-done; err != nil {
		msgsHandleFail.Inc(1)
		return err
	}

	msgsHandleOK.Inc(1)

	if k.totalMessages > 0 && k.messagesDone >= k.totalMessages {
		os.Exit(0)
	}
	return nil
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println("nsq_probe_events_to_elasticsearch")
		return
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal("--topic is required")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	metrics, err := helper.New(true, *statsdAddr, *statsdType, "nsq_probe_events_to_elasticsearch", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Don't ask for more messages than we want
	if *totalMessages > 0 && *totalMessages < *maxInFlight {
		*maxInFlight = *totalMessages
	}

	setting.Config = new(setting.Conf)
	setting.Config.ElasticsearchDomain = "elasticsearch"
	setting.Config.ElasticsearchPort = 9200

	eventsToEsOK = metrics.NewCount("events_to_es.ok")
	eventsToEsFail = metrics.NewCount("events_to_es.fail")
	messagesSize = metrics.NewMeter("message_size", 0)
	msgsAge = metrics.NewMeter("message_age", 0)
	esPutDuration = metrics.NewTimer("es_put_duration", 0)
	msgsHandleOK = metrics.NewCount("handle.ok")
	msgsHandleFail = metrics.NewCount("handle.fail")

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_probe_events_to_elasticsearch"
	err = app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "%s", metrics)

	if err != nil {
		log.Fatal(err)
	}

	handler, err := NewESHandler(*totalMessages)
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddHandler(handler)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Println("INFO starting listener for http/debug on :6060")
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
