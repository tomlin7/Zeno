package reactor

import (
	"sync/atomic"

	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/queue"
)

type Config struct {
	UseWorkers   bool
	WorkerRecvCh chan *item.Item

	UseQueue bool
	Queue    *queue.PersistentGroupedQueue

	UseHQ      bool
	HQProducer chan *item.Item
}

type reactor struct {
	// Signals
	stopCh chan struct{}
	doneCh chan struct{}

	// Hosts
	excludedHosts      []string
	addExcludedHostsCh chan []string
	rmExcludedHostsCh  chan []string
	includedHosts      []string
	addIncludedHostsCh chan []string
	rmIncludedHostsCh  chan []string

	// Reactor src and dest
	useQueue   bool
	useWorkers bool
	useHQ      bool

	// I/O
	queue     *queue.PersistentGroupedQueue
	captureRx chan *item.Item // Input channel from capture
	hqTx      chan *item.Item // Output channel to HQ
	workersTx chan *item.Item // Output channel to workers
	// TODO: Add queueTx/Rx when queue works with channels
	// queueRx chan *item.Item // Input channel from queue
	// queueTx chan *item.Item // Output channel to queue

	// State
	running *atomic.Bool
}

var (
	isInit         = false
	packageReactor *reactor
)

func Init(config *Config) {
	reactor := &reactor{
		stopCh:             make(chan struct{}),
		doneCh:             make(chan struct{}),
		excludedHosts:      []string{},
		addExcludedHostsCh: make(chan []string),
		rmExcludedHostsCh:  make(chan []string),
		includedHosts:      []string{},
		addIncludedHostsCh: make(chan []string),
		rmIncludedHostsCh:  make(chan []string),
		captureRx:          make(chan *item.Item),
		running:            new(atomic.Bool),
	}
	if config.UseWorkers {
		reactor.useWorkers = true
		reactor.workersTx = config.WorkerRecvCh
	}
	if config.UseQueue && config.Queue != nil {
		reactor.queue = config.Queue
		reactor.useQueue = true
	}
	if config.UseHQ {
		reactor.useHQ = true
		reactor.hqTx = config.HQProducer
	}
	if !isInit {
		packageReactor = reactor
		go reactor.run()
		isInit = true
	}
}

func AddExcludedHosts(hosts ...string) {
	packageReactor.addExcludedHostsCh <- hosts
}

func RemoveExcludedHosts(hosts ...string) {
	packageReactor.rmExcludedHostsCh <- hosts
}

func AddIncludedHosts(hosts ...string) {
	packageReactor.addIncludedHostsCh <- hosts
}

func RemoveIncludedHosts(hosts ...string) {
	packageReactor.rmIncludedHostsCh <- hosts
}

func Recv(item *item.Item) {
	packageReactor.captureRx <- item
}

func Stop() {
	packageReactor.stopCh <- struct{}{}
	<-packageReactor.doneCh
	close(packageReactor.doneCh)
}

func (p *reactor) run() {
	ok := p.running.CompareAndSwap(false, true)
	if !ok {
		return
	}
	defer func() { p.running.CompareAndSwap(true, false); isInit = false }()

	for {
		select {
		case <-p.stopCh:
			close(p.stopCh)
			close(p.addExcludedHostsCh)
			close(p.rmExcludedHostsCh)
			close(p.addIncludedHostsCh)
			close(p.rmIncludedHostsCh)
			p.doneCh <- struct{}{}
			return
		case hosts := <-p.addExcludedHostsCh:
			p.excludedHosts = append(p.excludedHosts, hosts...)
		case hosts := <-p.rmExcludedHostsCh:
			for _, host := range hosts {
				for i, excludedHost := range p.excludedHosts {
					if excludedHost == host {
						p.excludedHosts = append(p.excludedHosts[:i], p.excludedHosts[i+1:]...)
					}
				}
			}
		case hosts := <-p.addIncludedHostsCh:
			p.includedHosts = append(p.includedHosts, hosts...)
		case hosts := <-p.rmIncludedHostsCh:
			for _, host := range hosts {
				for i, includedHost := range p.includedHosts {
					if includedHost == host {
						p.includedHosts = append(p.includedHosts[:i], p.includedHosts[i+1:]...)
					}
				}
			}
		case item := <-p.captureRx:
			if item == nil || !p.checkHost(item) {
				continue
			}
			p.sendToProducers(item)
		default: // TODO : Turn default into a channel receive from queue when queue works with channels
			if p.useQueue {
				item, err := p.queue.Dequeue()
				if err != nil {
					continue
				}
				if item != nil {
					if !p.checkHost(item) {
						continue
					}
					p.workersTx <- item
				}
			}
		}
	}
}
