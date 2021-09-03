package dht

import (
	"fmt"
	"github.com/anacrolix/stm"
)

// Populates the Node Table.
func (s *Server) Bootstrap() (_ TraversalStats, err error) {
	t, err := s.newTraversal(s.id)
	if err != nil {
		return
	}
	t.reason = "dht bootstrap find_node"
	t.doneVar = stm.NewVar(false)
	// Track number of responses, for STM use. (It's available via atomic in TraversalStats but that
	// won't let wake up STM transactions that are observing the value.)
	numResponseStm := stm.NewBuiltinEqVar(0)
	t.stopTraversal = func(tx *stm.Tx, _ addrMaybeId) bool {
		return tx.Get(numResponseStm).(int) >= 100
	}
	t.query = func(addr Addr) QueryResult {
		res := s.FindNode(addr, s.id, QueryRateLimiting{NotFirst: true})
		if res.Err == nil {
			fmt.Printf("%+v\n", res)
			fmt.Printf("%+v\n", addr)

			stm.Atomically(stm.VoidOperation(func(tx *stm.Tx) {
				tx.Set(numResponseStm, tx.Get(numResponseStm).(int)+1)
			}))
		}
		return res
	}
	t.run()
	return t.stats, nil
}
