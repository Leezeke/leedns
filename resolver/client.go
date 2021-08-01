package resolver

import (
	"context"
	"math/rand"

	D "github.com/miekg/dns"
)

func (r *Resolver) copyClients() []*Client {
	clients := make([]*Client, len(r.Clients))
	copy(clients, r.Clients)
	return clients
}

func (r *Resolver) failedClient(c *Client) {
	c.failedTimes++
	if c.failedTimes < r.MaxRetries {
		return
	} else {
		if_reset := true
		for _, c := range r.Clients {
			if c.failedTimes < r.MaxRetries {
				if_reset = false
				break
			}
		}
		if if_reset {
			for _, c := range r.Clients {
				c.failedTimes = 0
			}
		}
	}
}

func (r *Resolver) recoverClient() {
	for _, c := range r.Clients {
		if c.failedTimes < r.MaxRetries {
			continue
		}
		m := new(D.Msg)
		m.SetQuestion(D.Fqdn("domain.com"), D.TypeA)
		_, _, err := c.c.Exchange(m)
		if err == nil {
			c.failedTimes = 0
		}
	}
}

func (r *Resolver) getClientByLoad() (c *Client, index int) {
	maxItemIndex := 0
	for index, c := range r.Clients {
		if c.currentWeight > r.Clients[maxItemIndex].currentWeight {
			maxItemIndex = index
		}
		c.currentWeight += c.Weight
	}
	c = r.Clients[maxItemIndex]
	c.currentWeight = c.currentWeight - r.weightSum

	c.count++

	return c, maxItemIndex
}

func concurrentQuery(m *D.Msg, r *Resolver) (msg *D.Msg, err error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		Msg   *D.Msg
		Error error
		c     *Client
	}

	ch := make(chan result, len(r.Clients))
	for _, c := range r.Clients {
		c := c
		if c.failedTimes >= r.MaxRetries {
			ch <- result{nil, nil, nil}
			continue
		}
		go func() {
			msg, _, err := c.c.ExchangeContext(ctx, m)
			ch <- result{msg, err, c}
		}()
	}

	var badResult *D.Msg
	for i := 0; i < len(r.Clients); i++ {
		ret := <-ch
		msg = ret.Msg
		err = ret.Error
		if err != nil || msg == nil {
			if ret.c != nil {
				r.failedClient(ret.c)
			}
			continue
		}
		if msg.Answer != nil {
			break
		} else {
			badResult = msg
		}
	}

	if msg == nil {
		msg = badResult
	}

	return msg, err
}

func randomQuery(m *D.Msg, r *Resolver) (msg *D.Msg, err error) {

	var badResult *D.Msg
	clients := r.copyClients()
	for i := len(clients) - 1; i >= 0; i-- {
		randIndex := rand.Intn(len(clients))
		c := clients[randIndex]

		if c.failedTimes >= r.MaxRetries {
			clients = append(clients[:randIndex], clients[randIndex+1:]...)
			continue
		}

		msg, _, err = c.c.Exchange(m)
		if err != nil || msg == nil {
			r.failedClient(c)
			clients = append(clients[:randIndex], clients[randIndex+1:]...)
			continue
		}
		if msg.Answer != nil {
			break
		} else {
			badResult = msg
		}
	}

	if msg == nil {
		msg = badResult
	}

	return msg, err
}

func loadBalancedQuery(m *D.Msg, r *Resolver) (msg *D.Msg, err error) {

	var badResult *D.Msg
	var lastClient *Client
	len := len(r.Clients)
	for i := len - 1; i >= 0; {
		c, _ := r.getClientByLoad()

		if c.failedTimes >= r.MaxRetries {
			continue
		}

		if c != lastClient {
			i--
			lastClient = c
		} else {
			continue
		}

		msg, _, err = c.c.Exchange(m)
		if err != nil || msg == nil {
			r.failedClient(c)
			continue
		}
		if msg.Answer != nil {
			break
		} else {
			badResult = msg
		}
	}

	if msg == nil {
		msg = badResult
	}

	return msg, err
}

func fallbackQuery(m *D.Msg, r *Resolver) (msg *D.Msg, err error) {
	var badResult *D.Msg
	for _, c := range r.Clients {
		if c.failedTimes >= r.MaxRetries {
			continue
		}

		msg, _, err = c.c.Exchange(m)
		if err != nil || msg == nil {
			r.failedClient(c)
			continue
		}
		if msg.Answer != nil {
			break
		} else {
			badResult = msg
		}
	}

	if msg == nil {
		msg = badResult
	}

	return msg, err
}
