package eth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/livepeer/node/destination"
)

const maxRPCResponse = 1 << 20

// RPC applies bounded HTTP transport and error redaction to geth's client.
type RPC struct {
	ethereum     *ethclient.Client
	http         *http.Client
	observer     func(string, time.Duration, error)
	callObserver func(contract, function string)
}

// SetObserver installs a hook before the client is used. Observations contain
// allowlisted methods and redacted errors, never RPC payloads or URLs.
func (r *RPC) SetObserver(observer func(string, time.Duration, error)) { r.observer = observer }

// SetCallObserver installs a hook before the client is used. It sees each eth_call,
// including those inside a batch, as labels from a fixed table of selectors.
func (r *RPC) SetCallObserver(observer func(contract, function string)) { r.callObserver = observer }

func OpenRPC(endpoint string) (*RPC, error) {
	u, err := destination.ParseURL(endpoint)
	if err != nil {
		return nil, errors.New("invalid Ethereum RPC URL")
	}
	return NewRPC(u, nil)
}

// NewRPC opens a bounded RPC client using the supplied transport. A nil
// transport uses the system HTTP transport, including normal TLS verification.
func NewRPC(endpoint *url.URL, transport http.RoundTripper) (*RPC, error) {
	if err := destination.ValidateURL(endpoint); err != nil {
		return nil, errors.New("invalid Ethereum RPC URL")
	}
	if transport == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	r := new(RPC)
	client := &http.Client{Transport: boundedRPCTransport{transport, r}, Timeout: 15 * time.Second}
	// Redirects can replay eth_sendRawTransaction POST bodies.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	rpc, err := gethrpc.DialOptions(context.Background(), endpoint.String(), gethrpc.WithHTTPClient(client))
	if err != nil {
		return nil, errors.New("invalid Ethereum RPC endpoint")
	}
	r.ethereum, r.http = ethclient.NewClient(rpc), client
	return r, nil
}

func (r *RPC) Close() {
	r.ethereum.Close()
	r.http.CloseIdleConnections()
}

var errRPCResponseLimit = errors.New("ethereum RPC response exceeds 1 MiB")

// Bound the HTTP body before geth decodes it or includes it in an HTTP error.
type boundedRPCTransport struct {
	http.RoundTripper
	rpc *RPC
}

func (t boundedRPCTransport) RoundTrip(request *http.Request) (response *http.Response, err error) {
	observer := t.rpc.observer
	var observed error
	batch := false
	if observer != nil || t.rpc.callObserver != nil {
		start, method := time.Now(), "unknown"
		if request.GetBody != nil {
			if body, err := request.GetBody(); err == nil {
				var requests []rpcRequestEnvelope
				requests, batch = decodeRPCRequests(body)
				if batch {
					method = "batch"
				} else if len(requests) == 1 && observedRPCMethods[requests[0].Method] {
					method = requests[0].Method
				}
				t.observeCalls(requests)
				_ = body.Close()
			}
		}
		defer func() {
			if err != nil {
				observed = err
			}
			if observer != nil {
				observer(method, time.Since(start), safeRPCError(observed))
			}
		}()
	}
	response, err = t.RoundTripper.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxRPCResponse+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxRPCResponse {
		return nil, errRPCResponseLimit
	}
	if observer != nil && (response.StatusCode < 200 || response.StatusCode >= 300 || !rpcResponseSucceeded(data, batch)) {
		observed = errors.New("ethereum RPC request failed")
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}

var observedRPCMethods = map[string]bool{"eth_chainId": true, "eth_call": true, "eth_getCode": true, "eth_blockNumber": true, "eth_getBalance": true, "eth_getTransactionCount": true, "eth_getTransactionReceipt": true, "eth_getTransactionByHash": true, "eth_getBlockByNumber": true, "eth_estimateGas": true, "eth_sendRawTransaction": true, "eth_maxPriorityFeePerGas": true, "eth_gasPrice": true}

type rpcRequestEnvelope struct {
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

// decodeRPCRequests reads a request body, which is an array when it is a batch.
func decodeRPCRequests(body io.Reader) ([]rpcRequestEnvelope, bool) {
	var raw json.RawMessage
	if json.NewDecoder(body).Decode(&raw) != nil {
		return nil, false
	}
	if len(raw) > 0 && raw[0] == '[' {
		var requests []rpcRequestEnvelope
		_ = json.Unmarshal(raw, &requests)
		return requests, true
	}
	var request rpcRequestEnvelope
	if json.Unmarshal(raw, &request) != nil {
		return nil, false
	}
	return []rpcRequestEnvelope{request}, false
}

func (t boundedRPCTransport) observeCalls(requests []rpcRequestEnvelope) {
	if t.rpc.callObserver == nil {
		return
	}
	for _, request := range requests {
		if request.Method != "eth_call" {
			continue
		}
		contract, function := "unknown", "unknown"
		var call struct {
			Input hexutil.Bytes `json:"input"`
			Data  hexutil.Bytes `json:"data"`
		}
		if len(request.Params) > 0 && json.Unmarshal(request.Params[0], &call) == nil {
			if len(call.Input) == 0 {
				call.Input = call.Data
			}
			if len(call.Input) >= 4 {
				if label, ok := contractCallLabels()[[4]byte(call.Input[:4])]; ok {
					contract, function = label[0], label[1]
				}
			}
		}
		t.rpc.callObserver(contract, function)
	}
}

// contractCallLabels is the fixed set of functions metrics name. It is built from
// the bindings' own selectors; anything else is reported as unknown.
var contractCallLabels = sync.OnceValue(func() map[[4]byte][2]string {
	labels := map[[4]byte][2]string{}
	for binding, functions := range map[string][]string{
		"controller":     {"getContract"},
		"roundsManager":  {"lastInitializedRound", "blockHashForRound"},
		"bondingManager": {"isActiveTranscoder"},
		"ticketBroker":   {"getSenderInfo", "claimableReserve", "usedTickets", "redeemWinningTicket"},
		"priceFeed":      {"description", "decimals", "latestRoundData"},
	} {
		parsed, err := bindingMetadata[binding].GetAbi()
		if err != nil {
			continue
		}
		for _, function := range functions {
			if method, ok := parsed.Methods[function]; ok {
				labels[[4]byte(method.ID)] = [2]string{binding, function}
			}
		}
	}
	return labels
})

func rpcResponseSucceeded(data []byte, batch bool) bool {
	type envelope struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if batch {
		var responses []envelope
		if json.Unmarshal(data, &responses) != nil {
			return false
		}
		return !slices.ContainsFunc(responses, func(response envelope) bool { return response.Error != nil })
	}
	var response envelope
	return json.Unmarshal(data, &response) == nil && response.Error == nil
}

func (t boundedRPCTransport) CloseIdleConnections() {
	if closer, ok := t.RoundTripper.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// geth errors may include an authenticated endpoint or server-supplied text.
func safeRPCError(err error) error {
	if err == nil {
		return nil
	}
	for _, safe := range []error{errRPCResponseLimit, context.Canceled, context.DeadlineExceeded, ethereum.NotFound} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	var httpError gethrpc.HTTPError
	if errors.As(err, &httpError) {
		return fmt.Errorf("ethereum RPC returned HTTP %d", httpError.StatusCode)
	}
	var rpcError gethrpc.Error
	if errors.As(err, &rpcError) {
		// Preserve only upstream's fixed terminal ticket classification. Arbitrary
		// provider text (including endpoints or calldata) must remain redacted.
		if strings.Contains(rpcError.Error(), "ticket creationRound does not have a block hash") {
			return ErrTicketCreationRound
		}
		return fmt.Errorf("ethereum RPC error %d", rpcError.ErrorCode())
	}
	return errors.New("ethereum RPC request failed")
}

var ErrTicketCreationRound = errors.New("ticket creationRound does not have a block hash")

func (r *RPC) TransactionByHash(ctx context.Context, hash ethcommon.Hash) (*types.Transaction, bool, error) {
	tx, pending, err := r.ethereum.TransactionByHash(ctx, hash)
	return tx, pending, safeRPCError(err)
}

func ValidAddress(address string) bool {
	if len(address) != 42 || !strings.HasPrefix(address, "0x") {
		return false
	}
	return ethcommon.IsHexAddress(address)
}

// ChainID uses ethclient's standard call and quantity decoding.
func (r *RPC) ChainID(ctx context.Context) (*big.Int, error) {
	id, err := r.ethereum.ChainID(ctx)
	return id, safeRPCError(err)
}

func (r *RPC) CodeAt(ctx context.Context, address ethcommon.Address, number *big.Int) ([]byte, error) {
	code, err := r.ethereum.CodeAt(ctx, address, number)
	return code, safeRPCError(err)
}

func (r *RPC) BlockNumber(ctx context.Context) (uint64, error) {
	number, err := r.ethereum.BlockNumber(ctx)
	return number, safeRPCError(err)
}

func (r *RPC) BalanceAt(ctx context.Context, address ethcommon.Address, number *big.Int) (*big.Int, error) {
	balance, err := r.ethereum.BalanceAt(ctx, address, number)
	return balance, safeRPCError(err)
}

func (r *RPC) PendingNonceAt(ctx context.Context, address ethcommon.Address) (uint64, error) {
	nonce, err := r.ethereum.PendingNonceAt(ctx, address)
	return nonce, safeRPCError(err)
}

func (r *RPC) TransactionReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	// Geth leaves Status unchanged when omitted/null; seed an invalid value.
	receipt := &types.Receipt{Status: 2}
	if err := r.ethereum.Client().CallContext(ctx, &receipt, "eth_getTransactionReceipt", hash); err != nil {
		return nil, safeRPCError(err)
	}
	if receipt == nil {
		return nil, ethereum.NotFound
	}
	if receipt.Status > types.ReceiptStatusSuccessful || receipt.TxHash != hash || receipt.BlockNumber == nil || !receipt.BlockNumber.IsUint64() || receipt.BlockHash == (ethcommon.Hash{}) {
		return nil, errors.New("invalid transaction receipt")
	}
	return receipt, nil
}

// miniHeader retains the RPC hash and Arbitrum's L1 clock, which geth's Header
// does not expose. All quantities use geth's JSON decoders.
type miniHeader struct {
	Number        *hexutil.Big   `json:"number"`
	BaseFee       *hexutil.Big   `json:"baseFeePerGas"`
	L1BlockNumber *hexutil.Big   `json:"l1BlockNumber"`
	Hash          ethcommon.Hash `json:"hash"`
}

func (r *RPC) header(ctx context.Context, block any) (*miniHeader, error) {
	var header *miniHeader
	if err := r.ethereum.Client().CallContext(ctx, &header, "eth_getBlockByNumber", block, false); err != nil {
		return nil, safeRPCError(err)
	}
	if header == nil {
		return nil, ethereum.NotFound
	}
	return header, nil
}

// CheckChainID validates the RPC chain ID, optionally pinning it to expected.
func (r *RPC) CheckChainID(ctx context.Context, expected *big.Int) error {
	id, err := r.ChainID(ctx)
	if err != nil {
		return err
	}
	if expected != nil && id.Cmp(expected) != 0 {
		return errors.New("RPC chain ID does not match configured chain-id")
	}
	return nil
}
