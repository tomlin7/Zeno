package reactor

import (
	"github.com/internetarchive/Zeno/internal/item"
)

func (p *reactor) sendToProducers(itemToSend *item.Item) {
	switch itemToSend.Type {
	case item.TypeSeed, item.TypeOutlink:
		select {
		case p.workersTx <- itemToSend:
			return
		default:
			if p.useHQ {
				p.hqTx <- itemToSend
			} else {
				// TODO: when queue works with channels, send to queue via channel
				err := p.queue.Enqueue(itemToSend)
				if err != nil {
					p.logger.Error("failed to enqueue item", "error", err)
				}
			}
		}
	case item.TypeAsset:
		if p.useWorkers {
			p.workersTx <- itemToSend
		}
	}
}
