package processer

import (
	"github.com/internetarchive/Zeno/internal/queue"
	"github.com/internetarchive/Zeno/internal/worker"
)

type Processer struct {
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

	// Queue/InputChan
	useQueue bool
	queue    *queue.PersistentGroupedQueue

	// Workers
	workersPool *worker.Pool
}

var (
	isInit           = false
	packageProcesser *Processer
)

func Init(workersPool *worker.Pool, queue *queue.PersistentGroupedQueue) {
	processer := &Processer{
		stopCh:             make(chan struct{}),
		doneCh:             make(chan struct{}),
		excludedHosts:      []string{},
		addExcludedHostsCh: make(chan []string),
		rmExcludedHostsCh:  make(chan []string),
		includedHosts:      []string{},
		addIncludedHostsCh: make(chan []string),
		rmIncludedHostsCh:  make(chan []string),
		queue:              queue,
		workersPool:        workersPool,
	}
	if !isInit {
		packageProcesser = processer
		go processer.run()
		isInit = true
	}
}

func AddExcludedHosts(hosts ...string) {
	packageProcesser.addExcludedHostsCh <- hosts
}

func RemoveExcludedHosts(hosts ...string) {
	packageProcesser.rmExcludedHostsCh <- hosts
}

func AddIncludedHosts(hosts ...string) {
	packageProcesser.addIncludedHostsCh <- hosts
}

func RemoveIncludedHosts(hosts ...string) {
	packageProcesser.rmIncludedHostsCh <- hosts
}

func Stop() {
	packageProcesser.stopCh <- struct{}{}
	<-packageProcesser.doneCh
	close(packageProcesser.doneCh)
}

func (p *Processer) run() {
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
		default:
			item, err := p.queue.Dequeue()
			if err != nil {
				continue
			}
			if item != nil {
				// if !p.checkHost(item.URL.Host) {
				// 	continue
				// }
				p.workersPool.Recv <- item
			}
		}
	}
}
