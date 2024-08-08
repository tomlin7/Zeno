package reactor

import (
	"github.com/internetarchive/Zeno/internal/item"
)

func (p *reactor) sendToProducers(itemToSend *item.Item) {
	switch itemToSend.Type {
	case item.TypeOutlink:
	case item.TypeSeed:
		if p.useHQ {
			p.hqTx <- itemToSend
		} else {
			// TODO: when queue works with channels, send to queue via channel
			p.queue.Enqueue(itemToSend)
		}
	case item.TypeAsset:
		if p.useWorkers {
			p.workersTx <- itemToSend
		}
	}
}
