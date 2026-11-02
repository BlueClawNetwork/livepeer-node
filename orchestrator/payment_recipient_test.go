package orchestrator

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

// These vectors were produced by running recipient.rand, expiration AuxData,
// and orchestrator.AuthToken extracted from the pinned go-livepeer revision.
// Those routines were authored by Yondon Fu and Nico Vergauwen; see payment_recipient.go.
func TestRecipientHMACMatchesGoLivepeer(t *testing.T) {
	data, err := os.ReadFile("testdata/recipient-hmac.json")
	require.NoError(t, err)
	var fixture struct {
		Revision string `json:"source_revision"`
		Secret   string
		Vectors  []struct {
			Seed, PayerAddress, FaceValue, WinProb, ExpirationBlock, Price, RoundHash, Session string
			Round, AuthExpiration                                                              int64
			Random, Commitment, Token                                                          string
		}
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Equal(t, "bd645a09266833fb859053445d9ac85846330756", fixture.Revision)
	secret, err := hex.DecodeString(fixture.Secret)
	require.NoError(t, err)
	e := &PaymentEngine{}
	copy(e.secret[:], secret)
	copy(e.authSecret[:], secret)
	num := func(s string) *big.Int {
		n, ok := new(big.Int).SetString(s, 10)
		require.True(t, ok)
		return n
	}
	for _, v := range fixture.Vectors {
		t.Run(v.Session, func(t *testing.T) {
			price, ok := new(big.Rat).SetString(v.Price)
			require.True(t, ok)
			random := e.recipientRand(num(v.Seed), ethcommon.HexToAddress(v.PayerAddress), num(v.FaceValue), num(v.WinProb), num(v.ExpirationBlock), price, &pm.TicketExpirationParams{CreationRound: v.Round, CreationRoundBlockHash: ethcommon.HexToHash(v.RoundHash)})
			raw := ethcommon.LeftPadBytes(random.Bytes(), 32)
			require.Equal(t, v.Random, hex.EncodeToString(raw))
			require.Equal(t, v.Commitment, crypto.Keccak256Hash(raw).Hex())
			require.Equal(t, v.Token, hex.EncodeToString(e.authToken(v.Session, v.AuthExpiration).Token))
		})
	}
}

type recipientTestChain struct {
	block                  int64
	active                 bool
	snapshotErr, activeErr error
	funds                  func(context.Context, pm.PayerFunds) (pm.PayerFunds, error)
}

func (c *recipientTestChain) Snapshot(context.Context) (pm.ChainSnapshot, error) {
	return pm.ChainSnapshot{Block: big.NewInt(c.block), Round: big.NewInt(5), RoundHash: ethcommon.HexToHash("0x1234")}, c.snapshotErr
}
func (c *recipientTestChain) IsActiveAt(context.Context, ethcommon.Address, pm.ChainSnapshot) (bool, error) {
	return c.active, c.activeErr
}
func (c *recipientTestChain) PayerFunds(ctx context.Context, _, _ ethcommon.Address) (pm.PayerFunds, error) {
	snapshot, err := c.Snapshot(ctx)
	funds := pm.PayerFunds{Snapshot: snapshot, Deposit: big.NewInt(1000000000), Reserve: big.NewInt(1000000000), WithdrawRound: new(big.Int)}
	if c.funds != nil {
		return c.funds(ctx, funds)
	}
	return funds, err
}

func newRecipientTestEngine(t *testing.T) (*PaymentEngine, *recipientTestChain, *ecdsa.PrivateKey, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "payments.sqlite")
	store, err := OpenRedeemerDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	chain := &recipientTestChain{active: true, block: 50}
	e, err := NewPaymentEngine(store, chain, ethcommon.HexToAddress("0x1234"), big.NewInt(10), new(big.Int).Sub(maxWinProb, big.NewInt(1)))
	require.NoError(t, err)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return e, chain, key, path
}

func issueRecipientParams(t *testing.T, e *PaymentEngine, key *ecdsa.PrivateKey) wire.OrchestratorInfo {
	t.Helper()
	challenge, err := e.MakeChallenge(t.Context(), "runner", "session", crypto.PubkeyToAddress(key.PublicKey), 9, "fixed", "https://orch.example")
	require.NoError(t, err)
	encoded, err := base64.StdEncoding.DecodeString(challenge.PaymentParams)
	require.NoError(t, err)
	info, err := wire.DecodeOrchestratorInfo(encoded)
	require.NoError(t, err)
	return info
}

func recipientPayment(t *testing.T, key *ecdsa.PrivateKey, info wire.OrchestratorInfo, nonces ...uint32) (wire.Payment, wire.SegData) {
	t.Helper()
	payer := crypto.PubkeyToAddress(key.PublicKey)
	p := info.TicketParams
	params := pm.TicketParams{Recipient: ethcommon.BytesToAddress(p.Recipient), FaceValue: new(big.Int).SetBytes(p.FaceValue), WinProb: new(big.Int).SetBytes(p.WinProb), RecipientRandHash: ethcommon.BytesToHash(p.RecipientRandHash), ExpirationBlock: new(big.Int).SetBytes(p.ExpirationBlock)}
	expiration := &pm.TicketExpirationParams{CreationRound: p.Expiration.CreationRound, CreationRoundBlockHash: ethcommon.BytesToHash(p.Expiration.CreationRoundBlockHash)}
	sign := func(msg []byte) []byte {
		sig, err := crypto.Sign(accounts.TextHash(msg), key)
		require.NoError(t, err)
		sig[64] += 27
		return sig
	}
	payment := wire.Payment{PayerAddress: payer.Bytes(), TicketParams: p, Expiration: p.Expiration, ExpectedPrice: info.Price}
	for _, nonce := range nonces {
		ticket := pm.NewTicket(&params, expiration, payer, nonce)
		payment.PayerParams = append(payment.PayerParams, wire.TicketPayerParams{TicketNonce: nonce, Sig: sign(ticket.Hash().Bytes())})
	}
	hash := crypto.Keccak256(nil)
	flatten := append([]byte(info.Auth.SessionID), make([]byte, 32)...)
	flatten = append(flatten, hash...)
	segment := wire.SegData{ManifestID: []byte(info.Auth.SessionID), Hash: hash, Signature: sign(crypto.Keccak256(flatten)), Auth: info.Auth}
	return payment, segment
}

func receiveRecipientPayment(t *testing.T, e *PaymentEngine, key *ecdsa.PrivateKey, info wire.OrchestratorInfo, nonces ...uint32) error {
	t.Helper()
	payment, segment := recipientPayment(t, key, info, nonces...)
	_, _, err := e.Receive(t.Context(), "runner", info.Auth.SessionID, base64.StdEncoding.EncodeToString(wire.EncodePayment(payment)), base64.StdEncoding.EncodeToString(wire.EncodeSegData(segment)))
	return err
}

func TestRecipientHMACExpirationCopies(t *testing.T) {
	e, _, key, _ := newRecipientTestEngine(t)
	info := issueRecipientParams(t, e, key)
	for _, copies := range []string{"both", "top-level", "nested"} {
		t.Run(copies, func(t *testing.T) {
			payment := wire.Payment{PayerAddress: crypto.PubkeyToAddress(key.PublicKey).Bytes(), TicketParams: info.TicketParams, Expiration: info.TicketParams.Expiration, ExpectedPrice: info.Price}
			if copies == "top-level" {
				// go-livepeer omits the nested copy.
				payment.TicketParams.Expiration = wire.ExpirationParams{}
			} else if copies == "nested" {
				payment.Expiration = wire.ExpirationParams{}
			}
			payment, err := wire.DecodePayment(wire.EncodePayment(payment))
			require.NoError(t, err)
			params, _, err := e.authenticatePayment(payment, info.Auth)
			require.NoError(t, err)
			require.Equal(t, info.TicketParams.Expiration.CreationRound, params.ExpirationParams.CreationRound)
			require.Equal(t, info.TicketParams.Expiration.CreationRoundBlockHash, params.ExpirationParams.CreationRoundBlockHash.Bytes())
			if copies != "nested" {
				payment.Expiration.CreationRound++
			}
			if copies != "top-level" {
				payment.TicketParams.Expiration.CreationRound++
			}
			_, _, err = e.authenticatePayment(payment, info.Auth)
			require.ErrorIs(t, err, ErrInvalidPayment, "the selected expiration must be bound by the HMAC")
		})
	}
}

func TestRecipientHMACRejectsTamperedParameters(t *testing.T) {
	e, _, key, _ := newRecipientTestEngine(t)
	original := issueRecipientParams(t, e, key)
	for name, mutate := range map[string]func(*wire.Payment, *wire.AuthToken){
		"payer":             func(p *wire.Payment, _ *wire.AuthToken) { p.PayerAddress[0] ^= 1 },
		"recipient":         func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.Recipient[0] ^= 1 },
		"seed":              func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.Seed[0] ^= 1 },
		"face value":        func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.FaceValue = big.NewInt(11).Bytes() },
		"win probability":   func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.WinProb = big.NewInt(100).Bytes() },
		"expiry block":      func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.ExpirationBlock = big.NewInt(91).Bytes() },
		"price numerator":   func(p *wire.Payment, _ *wire.AuthToken) { p.ExpectedPrice.PricePerUnit++ },
		"price denominator": func(p *wire.Payment, _ *wire.AuthToken) { p.ExpectedPrice.UnitsPerPrice++ },
		"no expiration": func(p *wire.Payment, _ *wire.AuthToken) {
			p.Expiration = wire.ExpirationParams{}
			p.TicketParams.Expiration = wire.ExpirationParams{}
		},
		"top round missing": func(p *wire.Payment, _ *wire.AuthToken) { p.Expiration.CreationRound = 0 },
		"top hash missing":  func(p *wire.Payment, _ *wire.AuthToken) { p.Expiration.CreationRoundBlockHash = nil },
		"split expiration": func(p *wire.Payment, _ *wire.AuthToken) {
			p.Expiration.CreationRoundBlockHash = nil
			p.TicketParams.Expiration.CreationRound = 0
		},
		"round conflict": func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.Expiration.CreationRound++ },
		"hash conflict": func(p *wire.Payment, _ *wire.AuthToken) {
			p.TicketParams.Expiration.CreationRoundBlockHash = ethcommon.HexToHash("0x4321").Bytes()
		},
		"round omitted": func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.Expiration.CreationRound = 0 },
		"hash omitted":  func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.Expiration.CreationRoundBlockHash = nil },
		"round": func(p *wire.Payment, _ *wire.AuthToken) {
			p.Expiration.CreationRound++
			p.TicketParams.Expiration.CreationRound++
		},
		"round hash": func(p *wire.Payment, _ *wire.AuthToken) {
			p.Expiration.CreationRoundBlockHash = ethcommon.HexToHash("0x4321").Bytes()
			p.TicketParams.Expiration = p.Expiration
		},
		"commitment":   func(p *wire.Payment, _ *wire.AuthToken) { p.TicketParams.RecipientRandHash[0] ^= 1 },
		"auth token":   func(_ *wire.Payment, a *wire.AuthToken) { a.Token[0] ^= 1 },
		"auth expiry":  func(_ *wire.Payment, a *wire.AuthToken) { a.Expiration++ },
		"auth session": func(_ *wire.Payment, a *wire.AuthToken) { a.SessionID += "-forged" },
	} {
		t.Run(name, func(t *testing.T) {
			info, err := wire.DecodeOrchestratorInfo(wire.EncodeOrchestratorInfo(original))
			require.NoError(t, err)
			p := wire.Payment{PayerAddress: crypto.PubkeyToAddress(key.PublicKey).Bytes(), TicketParams: info.TicketParams, Expiration: info.TicketParams.Expiration, ExpectedPrice: info.Price}
			_, _, err = e.authenticatePayment(p, info.Auth)
			require.NoError(t, err)
			mutate(&p, &info.Auth)
			_, _, err = e.authenticatePayment(p, info.Auth)
			require.ErrorIs(t, err, ErrInvalidPayment)
		})
	}
}

func TestRecipientBatchReplayAndExpiry(t *testing.T) {
	e, chain, key, _ := newRecipientTestEngine(t)
	e.metrics = observability.New("orchestrator")
	info := issueRecipientParams(t, e, key)
	require.ErrorIs(t, receiveRecipientPayment(t, e, key, info, 1, 1), ErrInvalidPayment)
	balance, err := e.Balance("session")
	require.NoError(t, err)
	require.Zero(t, balance.Sign())
	count, err := e.store.WinningTicketCount(crypto.PubkeyToAddress(key.PublicKey), 0)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, receiveRecipientPayment(t, e, key, info, 1), "a failed batch must not consume nonces")
	balance, err = e.Balance("session")
	require.NoError(t, err)
	balance.SetInt64(-1)
	before, err := e.Balance("session")
	require.NoError(t, err)
	require.Positive(t, before.Sign(), "callers must not mutate accounting state")
	challenges := test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "challenge"})
	_, err = e.RefreshChallenge(t.Context(), "session", crypto.PubkeyToAddress(key.PublicKey), "https://orch.example")
	require.NoError(t, err)
	require.Equal(t, challenges, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "challenge"}))
	require.EqualValues(t, 1, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "refresh", "outcome": "success"}))
	// A newer valid auth token cannot reset nonce tracking for old PM params.
	info.Auth = e.authToken("session", time.Now().Add(2*time.Hour).Unix())
	require.ErrorIs(t, receiveRecipientPayment(t, e, key, info, 1), ErrInvalidPayment)
	after, err := e.Balance("session")
	require.NoError(t, err)
	require.Equal(t, before.RatString(), after.RatString(), "replay cannot add credit")
	require.NoError(t, receiveRecipientPayment(t, e, key, info, 2), "unexpired prior parameters still authenticate after refresh")
	chain.block = 90
	require.NoError(t, e.PruneControlState(t.Context()))
	require.Empty(t, e.ticketNonces)
	require.Zero(t, e.nonceCount)
	chain.block = 89
	require.ErrorIs(t, receiveRecipientPayment(t, e, key, info, 1), ErrInvalidPayment, "a backward observation cannot revive pruned replay state")
	newInfo := issueRecipientParams(t, e, key)
	require.NoError(t, receiveRecipientPayment(t, e, key, newInfo, 1))
	require.EqualValues(t, 3, test.MetricSum(t, e.metrics.Registry, "payment_tickets_total", map[string]string{"kind": "accepted"}))
	require.EqualValues(t, 3, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "receive", "outcome": "success"}))
}

func TestPaymentStepDurations(t *testing.T) {
	e, _, key, _ := newRecipientTestEngine(t)
	e.metrics = observability.New("orchestrator")
	info := issueRecipientParams(t, e, key)
	require.NoError(t, receiveRecipientPayment(t, e, key, info, 1))
	require.ErrorIs(t, receiveRecipientPayment(t, e, key, info, 1), ErrInvalidPayment)
	_, err := e.RefreshChallenge(t.Context(), "session", crypto.PubkeyToAddress(key.PublicKey), "https://orch.example")
	require.NoError(t, err)
	for operation, count := range map[string]int{"challenge": 1, "receive": 2, "refresh": 1} {
		require.EqualValues(t, count, test.MetricSum(t, e.metrics.Registry, "payment_operation_duration_seconds", map[string]string{"operation": operation}), operation)
	}
}

func TestRecipientRestartRotatesSecretsAndPreservesWinningTickets(t *testing.T) {
	e, chain, key, path := newRecipientTestEngine(t)
	info := issueRecipientParams(t, e, key)
	require.NoError(t, receiveRecipientPayment(t, e, key, info, 1))
	winner, err := e.store.SelectEarliestWinningTicket(crypto.PubkeyToAddress(key.PublicKey), 0)
	require.NoError(t, err)
	require.NotNil(t, winner)
	require.NoError(t, e.store.Close())
	store, err := OpenRedeemerDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	restarted, err := NewPaymentEngine(store, chain, e.recipient, e.faceValue, e.winProb)
	require.NoError(t, err)
	_, err = restarted.ChallengeInfo("session")
	require.ErrorIs(t, err, ErrMissingChallenge)
	fresh := issueRecipientParams(t, restarted, key)
	// Even a current auth token cannot authenticate the prior recipient's params.
	previousAuth := info.Auth
	info.Auth = fresh.Auth
	require.ErrorIs(t, receiveRecipientPayment(t, restarted, key, info, 1), ErrInvalidPayment)
	retained, err := store.SelectEarliestWinningTicket(crypto.PubkeyToAddress(key.PublicKey), 0)
	require.NoError(t, err)
	require.Equal(t, winner, retained)
	// Conversely, current PM params cannot reuse the previous auth token.
	fresh.Auth = previousAuth
	require.ErrorIs(t, receiveRecipientPayment(t, restarted, key, fresh, 1), ErrInvalidPayment)
	fresh.Auth = info.Auth
	require.NoError(t, receiveRecipientPayment(t, restarted, key, fresh, 1))
}

func TestInactiveOrchestratorDoesNotIssuePaidChallenge(t *testing.T) {
	e, chain, key, _ := newRecipientTestEngine(t)
	chain.active = false
	_, err := e.MakeChallenge(t.Context(), "runner", "session", crypto.PubkeyToAddress(key.PublicKey), 9, "fixed", "https://orch.example")
	require.ErrorContains(t, err, "not active")
}

func TestRecipientRejectsParametersOnceRedemptionPrepared(t *testing.T) {
	e, _, key, _ := newRecipientTestEngine(t)
	info := issueRecipientParams(t, e, key)
	require.NoError(t, receiveRecipientPayment(t, e, key, info, 1))
	before, err := e.Balance("session")
	require.NoError(t, err)
	ticket, err := e.store.SelectEarliestWinningTicket(crypto.PubkeyToAddress(key.PublicKey), 0)
	require.NoError(t, err)
	require.NotNil(t, ticket)
	// A redemption may reveal the randomness even when this recipient's chain
	// observation still precedes parameter expiry.
	require.NoError(t, e.store.recordPrepared(t.Context(), ticket, eth.SignedTransaction{Hash: ethcommon.HexToHash("0x1234"), Raw: []byte{1}, From: e.recipient, Nonce: 1}))
	require.ErrorIs(t, receiveRecipientPayment(t, e, key, info, 2), ErrInvalidPayment)
	after, err := e.Balance("session")
	require.NoError(t, err)
	require.Equal(t, before.RatString(), after.RatString())
}

func TestRecipientStorageFailureDoesNotPublishCreditOrConsumeNonces(t *testing.T) {
	e, _, key, _ := newRecipientTestEngine(t)
	e.metrics = observability.New("orchestrator")
	info := issueRecipientParams(t, e, key)
	_, err := e.store.db.Exec(`CREATE TRIGGER reject_second_ticket BEFORE INSERT ON winning_tickets
		WHEN NEW.ticket_nonce=2 BEGIN SELECT RAISE(ABORT, 'storage failure'); END`)
	require.NoError(t, err)
	require.ErrorContains(t, receiveRecipientPayment(t, e, key, info, 1, 2), "storage failure")
	require.EqualValues(t, 1, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "receive", "outcome": "error"}))
	require.Zero(t, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "receive", "outcome": "success"}))
	count, err := e.store.WinningTicketCount(crypto.PubkeyToAddress(key.PublicKey), 0)
	require.NoError(t, err)
	require.Zero(t, count, "partial inserts must roll back")
	balance, err := e.Balance("session")
	require.NoError(t, err)
	require.Zero(t, balance.Sign())
	require.Zero(t, e.nonceCount)
	_, err = e.store.db.Exec(`DROP TRIGGER reject_second_ticket`)
	require.NoError(t, err)
	require.NoError(t, receiveRecipientPayment(t, e, key, info, 1, 2), "storage failure must leave both nonces retryable")
	count, err = e.store.WinningTicketCount(crypto.PubkeyToAddress(key.PublicKey), 0)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestPaidFailureOutcomes(t *testing.T) {
	for _, cause := range []string{
		"funding", "withdrawal", "face_value", "liability", "capacity", "rpc", "storage",
		"cancelled", "unhashed segment", "tampered hash", "challenge snapshot",
		"challenge active", "challenge cancelled", "challenge timeout", "challenge capacity",
	} {
		t.Run(cause, func(t *testing.T) {
			e, chain, key, _ := newRecipientTestEngine(t)
			registry, server := reviewServer(t, "http://127.0.0.1:1")
			defer server.Close()
			registry.SetWeiPerUSD(big.NewRat(1, 1))
			require.NoError(t, registry.AddStatic(StaticRunner{ID: "runner", App: "model", Mode: "single-shot", RunnerURL: "http://127.0.0.1:1", Capacity: 1, PriceInfo: priceInfo{Price: "9", Currency: "usd", Unit: "fixed"}}))
			server.SetPayment(e)
			info := issueRecipientParams(t, e, key)
			nonces := []uint32{1}
			operation, outcome, appOutcome := "receive", "rejected", "rejected"
			switch cause {
			case "capacity":
				e.nonceCount = 1000000
				outcome, appOutcome = "capacity", "capacity"
			case "rpc", "storage":
				outcome, appOutcome = "error", "error"
			case "cancelled":
				outcome, appOutcome = "cancelled", "client_cancelled"
			case "challenge capacity":
				for n := range 100000 {
					e.sessions[strconv.Itoa(n)] = nil
				}
				operation, outcome, appOutcome = "challenge", "capacity", "capacity"
			case "challenge snapshot":
				chain.snapshotErr = syscall.ECONNREFUSED
				operation, outcome, appOutcome = "challenge", "connection_refused", "error"
			case "challenge active":
				chain.activeErr = errors.New("RPC unavailable")
				operation, outcome, appOutcome = "challenge", "error", "error"
			case "challenge cancelled":
				chain.snapshotErr = context.Canceled
				operation, outcome, appOutcome = "challenge", "cancelled", "client_cancelled"
			case "challenge timeout":
				chain.snapshotErr = context.DeadlineExceeded
				operation, outcome, appOutcome = "challenge", "timeout", "error"
			case "liability":
				require.NoError(t, receiveRecipientPayment(t, e, key, info, 1))
				nonces = []uint32{2, 3}
			}
			chain.funds = func(ctx context.Context, funds pm.PayerFunds) (pm.PayerFunds, error) {
				switch cause {
				case "funding":
					funds.Reserve.SetInt64(0)
				case "withdrawal":
					funds.WithdrawRound.Set(funds.Snapshot.Round)
				case "face_value":
					funds.Deposit.SetInt64(1)
				case "liability":
					funds.Deposit.SetInt64(10)
					funds.Reserve.SetInt64(10)
				case "rpc":
					return pm.PayerFunds{}, errors.New("RPC unavailable")
				case "cancelled":
					return pm.PayerFunds{}, ctx.Err()
				}
				return funds, nil
			}
			if cause == "storage" {
				_, err := e.store.db.Exec(`CREATE TRIGGER reject_ticket BEFORE INSERT ON winning_tickets BEGIN SELECT RAISE(ABORT,'storage failure'); END`)
				require.NoError(t, err)
			}
			status := 400
			request := httptest.NewRequest("POST", "/apps/runner/app", nil)
			if operation == "receive" {
				payment, segment := recipientPayment(t, key, info, nonces...)
				if cause == "unhashed segment" {
					flatten := append(append([]byte(info.Auth.SessionID), make([]byte, 32)...), segment.Hash...)
					signature, err := crypto.Sign(accounts.TextHash(flatten), key)
					require.NoError(t, err)
					signature[64] += 27
					segment.Signature, status = signature, 403
				} else if cause == "tampered hash" {
					segment.Hash[0] ^= 1
					status = 403
				}
				request.Header.Set("Livepeer-Payment", base64.StdEncoding.EncodeToString(wire.EncodePayment(payment)))
				request.Header.Set("Livepeer-Segment", base64.StdEncoding.EncodeToString(wire.EncodeSegData(segment)))
			} else {
				status = 502
				request.Header.Set("Livepeer-Payer-Address", crypto.PubkeyToAddress(key.PublicKey).Hex())
			}
			if cause == "cancelled" {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				request = request.WithContext(ctx)
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			require.Equal(t, status, response.Code, response.Body.String())
			require.EqualValues(t, 1, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": operation, "outcome": outcome}))
			require.EqualValues(t, 1, test.MetricSum(t, e.metrics.Registry, "app_calls_total", map[string]string{"outcome": appOutcome}))
			require.Zero(t, test.MetricSum(t, e.metrics.Registry, "upstream_requests_total", nil))
		})
	}

}

func TestLiveChargeObservesOnlyDebitAttempts(t *testing.T) {
	e, _, key, _ := newRecipientTestEngine(t)
	e.metrics = observability.New("orchestrator")
	_, err := e.MakeChallenge(t.Context(), "runner", "session", crypto.PubkeyToAddress(key.PublicKey), 9, "seconds", "https://orch.example")
	require.NoError(t, err)
	e.sessions["session"].balance.SetInt64(18)
	now := time.Now()
	require.NoError(t, e.Charge(t.Context(), "session", now))
	for range 3 {
		require.NoError(t, e.Charge(t.Context(), "session", now.Add(100*time.Millisecond)))
	}
	require.Zero(t, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "charge"}))
	require.NoError(t, e.Charge(t.Context(), "session", now.Add(time.Second)))
	require.ErrorIs(t, e.Charge(t.Context(), "session", now.Add(3*time.Second)), ErrInsufficientBalance)
	require.EqualValues(t, 1, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "charge", "outcome": "success"}))
	require.EqualValues(t, 1, test.MetricSum(t, e.metrics.Registry, "payment_operations_total", map[string]string{"operation": "charge", "outcome": "insufficient_balance"}))
}
