package memory_test

import (
	"testing"

	"github.com/gopherex/synclog/internal/storage/contract"
	"github.com/gopherex/synclog/internal/storage/memory"
)

func TestStoreContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) contract.Store {
		t.Helper()
		return memory.NewStore()
	})
}
