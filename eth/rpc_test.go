package eth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRPCTransportPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		respond func(http.ResponseWriter, *http.Request)
		error   string
	}{
		{"bounded response", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(bytes.Repeat([]byte("x"), maxRPCResponse+1))
		}, "exceeds 1 MiB"},
		{"HTTP error redaction", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "secret-password secret-token", http.StatusServiceUnavailable)
		}, "HTTP 503"},
		{"RPC error redaction", func(w http.ResponseWriter, r *http.Request) {
			var request struct{ ID json.RawMessage }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32000, "message": "secret-password secret-token"}})
		}, "RPC error -32000"},
		{"redirect is not replayed", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/replayed" {
				t.Error("RPC request followed a redirect")
			}
			http.Redirect(w, r, "/replayed", http.StatusTemporaryRedirect)
		}, "HTTP 307"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tc.respond))
			defer server.Close()
			parsed, err := url.Parse(server.URL + "?token=secret-token")
			require.NoError(t, err)
			parsed.User = url.UserPassword("operator", "secret-password")
			rpc, err := NewRPC(parsed, nil)
			require.NoError(t, err)
			defer rpc.Close()
			observed := 0
			rpc.SetObserver(func(method string, elapsed time.Duration, observedErr error) {
				observed++
				require.Equal(t, "eth_chainId", method)
				require.GreaterOrEqual(t, elapsed, time.Duration(0))
				require.Error(t, observedErr)
				require.NotContains(t, observedErr.Error(), "secret-")
			})
			_, err = rpc.ChainID(t.Context())
			require.Equal(t, 1, observed)

			require.ErrorContains(t, err, tc.error)
			require.NotContains(t, err.Error(), "secret-password")
			require.NotContains(t, err.Error(), "secret-token")
		})
	}
}

func TestOptionalChainID(t *testing.T) {
	for _, tc := range []struct {
		expected *big.Int
		valid    bool
	}{
		{nil, true}, {big.NewInt(42161), true}, {big.NewInt(1), false},
	} {
		t.Run(fmt.Sprint(tc.expected), func(t *testing.T) {
			rpc := testRPC(t, func(method string, _ []json.RawMessage) any {
				require.Equal(t, "eth_chainId", method)
				return "0xa4b1"
			})
			err := rpc.CheckChainID(t.Context(), tc.expected)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// testRPC keeps adapter tests focused on their requests and responses.
func testRPC(t *testing.T, reply func(string, []json.RawMessage) any) *RPC {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage
			Method string
			Params []json.RawMessage
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": reply(req.Method, req.Params)}))
	}))
	t.Cleanup(server.Close)
	rpc, err := OpenRPC(server.URL)
	require.NoError(t, err)
	t.Cleanup(rpc.Close)
	return rpc
}

type ticketRevertRPCError struct{ message string }

func (e ticketRevertRPCError) Error() string  { return e.message }
func (e ticketRevertRPCError) ErrorCode() int { return -32000 }

func TestTerminalTicketErrorRetainsOnlyKnownClassification(t *testing.T) {
	original := ticketRevertRPCError{message: "secret https://user:password@rpc.example: ticket creationRound does not have a block hash; private calldata"}
	require.ErrorIs(t, safeRPCError(original), ErrTicketCreationRound)
	require.Equal(t, "ticket creationRound does not have a block hash", safeRPCError(original).Error())
	require.Equal(t, "ethereum RPC error -32000", safeRPCError(ticketRevertRPCError{message: "nonce too low https://user:password@rpc.example"}).Error())
}

func TestRPCObserverCountsBatchesOnceAndContractCallsEach(t *testing.T) {
	rpc := fundedViewChain(t)
	type observation struct {
		method string
		err    error
	}
	var mu sync.Mutex
	var trips []observation
	calls := map[[2]string]int{}
	view, _ := newTestView(t, rpc, func(options *ChainViewOptions) {})
	client := view.chain.Contracts.RPC
	client.SetObserver(func(method string, _ time.Duration, err error) {
		mu.Lock()
		defer mu.Unlock()
		trips = append(trips, observation{method, err})
	})
	client.SetCallObserver(func(contract, function string) {
		mu.Lock()
		defer mu.Unlock()
		calls[[2]string{contract, function}]++
	})
	require.NoError(t, view.Refresh(t.Context()))
	_, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	mu.Lock()
	trips, calls = nil, map[[2]string]int{}
	mu.Unlock()
	require.NoError(t, view.Refresh(t.Context()))
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []observation{{"eth_getBlockByNumber", nil}, {"batch", nil}}, trips, "an HTTP request counts once, and a batch response is not a failure")
	require.Equal(t, map[[2]string]int{
		{"controller", "getContract"}: 3, {"roundsManager", "lastInitializedRound"}: 1, {"roundsManager", "blockHashForRound"}: 1,
		{"bondingManager", "isActiveTranscoder"}: 1, {"ticketBroker", "getSenderInfo"}: 1, {"ticketBroker", "claimableReserve"}: 1,
	}, calls, "each call inside a batch counts")
}

func TestRPCObserverSeesDirectCallsAndFailedBatches(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	client := view.chain.Contracts.RPC
	calls := map[[2]string]int{}
	var failures []string
	client.SetCallObserver(func(contract, function string) { calls[[2]string{contract, function}]++ })
	client.SetObserver(func(method string, _ time.Duration, err error) {
		if err != nil {
			failures = append(failures, method)
		}
	})
	_, err := view.chain.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[[2]string]int{{"controller", "getContract"}: 1, {"roundsManager", "lastInitializedRound"}: 1, {"roundsManager", "blockHashForRound"}: 1}, calls)
	require.Empty(t, failures)

	clear(calls)
	var result string
	_ = client.ethereum.Client().CallContext(t.Context(), &result, "eth_call", map[string]any{"to": viewPayerAddress, "input": "0xdeadbeef"}, "latest")
	require.Equal(t, map[[2]string]int{{"unknown", "unknown"}: 1}, calls, "a selector outside the table cannot create labels")

	rpc.FailCalls(func(string, []byte) bool { return true })
	_ = view.Refresh(t.Context())
	require.Contains(t, failures, "batch", "a batch holding an error is a failed observation")
}

func TestContractCallLabelsNameEveryFunction(t *testing.T) {
	require.Len(t, contractCallLabels(), 11, "every listed function exists in its binding")
}
