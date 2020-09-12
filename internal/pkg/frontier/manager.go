package frontier

import (
	log "github.com/sirupsen/logrus"
)

func (f *Frontier) writeItemsToQueue() {
	for item := range f.PushChan {
		// Check if host is in the pool, if it is not, we add it
		// if it is, we increment its counter
		f.HostPool.Mutex.Lock()
		if f.HostPool.IsHostInPool(item.Host) == false {
			f.HostPool.Add(item.Host)
		} else {
			f.HostPool.Incr(item.Host)
		}
		f.HostPool.Mutex.Unlock()

		// Add the item to the host's queue
		_, err := f.Queue.EnqueueObject([]byte(item.Host), item)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Unable to enqueue item")
		}

		log.WithFields(log.Fields{
			"url": item.URL,
		}).Debug("Item enqueued")
	}
}

func (f *Frontier) readItemsFromQueue() {
	for {
		f.HostPool.Mutex.Lock()
		for _, host := range f.HostPool.Hosts {
			if host.Count.Value() == 0 {
				continue
			}

			// Dequeue an item from the local queue
			queueItem, err := f.Queue.Dequeue([]byte(host.Host))
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Debug("Unable to dequeue item")
				continue
			}

			// Turn the item from the queue into an Item
			var item *Item
			err = queueItem.ToObject(&item)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Error("Unable to parse queue's item")
				continue
			}

			// Sending the item to the workers via PullChan
			f.PullChan <- item
			log.WithFields(log.Fields{
				"url": item.URL,
			}).Debug("Item sent to workers pool")

			host.Count.Incr(-1)
		}
		f.HostPool.Mutex.Unlock()
	}
}