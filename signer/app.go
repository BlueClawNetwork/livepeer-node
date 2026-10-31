package signer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os/signal"
	"sync"
	"syscall"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/cmd/version"
	"github.com/livepeer/node/destination"
	discovery "github.com/livepeer/node/discovery/shared"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/nodeconfig"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/livepeer/node/pm"
	"github.com/spf13/cobra"
)

type Params struct {
	ConfigFile string `name:"config" configfile:"optional-default" basepath:"source" default:"config.toml" boa:"noconfig" descr:"Configuration file; empty disables discovery"`
	nodeconfig.Settings
	Account                     *ethcommon.Address   `descr:"Keystore account address"`
	Kafka                       *KafkaConfig         `optional:"true"`
	AuthWebhookHeadersFile      string               `basepath:"source" name:"auth-webhook-headers-file" secretfor:"AuthWebhookHeaders"`
	AuthWebhook                 *url.URL             `name:"auth-webhook" optional:"true" secret:"true"`
	AuthWebhookFile             string               `basepath:"source" name:"auth-webhook-file" secretfor:"AuthWebhook"`
	AuthWebhookHeaders          Headers              `name:"auth-webhook-headers" optional:"true" secret:"true"`
	MaxTicketEV                 *big.Rat             `name:"max-ticket-ev" default:"3000000000000" descr:"Maximum expected ticket value in wei"`
	MaxBatchEV                  *big.Rat             `name:"max-batch-ev" default:"20000000000000" descr:"Maximum expected batch value in wei"`
	DepositMultiplier           int64                `name:"deposit-multiplier" default:"1" min:"1" descr:"Maximum face value is deposit divided by this value"`
	MaxHourlyPrice              *big.Rat             `name:"max-hourly-price" required:"true" descr:"Maximum live price in USD per hour"`
	MaxFixedPrice               *big.Rat             `name:"max-fixed-price" required:"true" descr:"Maximum fixed request price in USD"`
	WeiPerUSD                   *big.Rat             `name:"wei-per-usd" optional:"true" descr:"Fixed wei per USD conversion, mostly for testing"`
	ETHUSDFeed                  ethcommon.Address    `name:"eth-usd-feed" optional:"true" descr:"ETH/USD oracle address"`
	ETHUSDMaxAge                time.Duration        `name:"eth-usd-max-age" default:"2h" descr:"Maximum age of the ETH/USD oracle observation"`
	Listen                      netip.AddrPort       `name:"listen" default:"127.0.0.1:8937"`
	MetricsListen               netip.AddrPort       `name:"metrics-listen" default:"127.0.0.1:8938"`
	RPCURL                      *url.URL             `name:"rpc-url" secret:"true" required:"true"`
	RPCURLFile                  string               `basepath:"source" name:"rpc-url-file" secretfor:"RPCURL"`
	ChainID                     *uint64              `name:"chain-id" min:"1" descr:"Optional expected RPC chain ID"`
	Controller                  ethcommon.Address    `name:"controller-address" optional:"true" descr:"Livepeer Controller address"`
	KeystoreFile                string               `basepath:"source" optional:"true" descr:"Encrypted geth account JSON file; otherwise discover in keystore/"`
	KeystorePassword            *string              `secret:"true" required:"true" descr:"Keystore decryption password"`
	KeystorePasswordFile        string               `basepath:"source" secretfor:"KeystorePassword" descr:"Owner-only file containing the exact keystore password bytes"`
	Orchestrators               []boa.Text[*url.URL] `name:"orchestrators" optional:"true"`
	DiscoveryRefreshInterval    time.Duration        `default:"5m" descr:"Discovery inventory refresh interval"`
	DiscoveryMaxFailedRefreshes int                  `default:"6" min:"0" descr:"Consecutive failed discovery refreshes before eviction"`
	DiscoveryGrants             []string             `name:"discovery-grants" optional:"true" descr:"Private discovery IP[:port] or CIDR grants"`

	OrchestratorWebhook                *url.URL      `name:"orchestrator-webhook" optional:"true" secret:"true"`
	OrchestratorWebhookFile            string        `basepath:"source" name:"orchestrator-webhook-file" secretfor:"OrchestratorWebhook"`
	OrchestratorWebhookHeaders         Headers       `name:"orchestrator-webhook-headers" optional:"true" secret:"true"`
	OrchestratorWebhookHeadersFile     string        `basepath:"source" name:"orchestrator-webhook-headers-file" secretfor:"OrchestratorWebhookHeaders"`
	OrchestratorWebhookRefreshInterval time.Duration `name:"orchestrator-webhook-refresh-interval" default:"5m" descr:"Orchestrator webhook refresh interval; must be positive when enabled"`

	ChainRefreshInterval time.Duration `default:"5s" descr:"Chain view refresh interval for payments; 0 reads the chain on every request"`
	ChainMaxAge          time.Duration `default:"30s" descr:"Refuse payments when the chain view is older than this; must exceed chain-refresh-interval"`
}

func (p Params) payerPolicy() (pm.PayerPolicy, error) {
	policy := pm.DefaultPayerPolicy()
	if p.MaxTicketEV != nil {
		policy.MaxTicketEV = p.MaxTicketEV
	}
	if p.MaxBatchEV != nil {
		policy.MaxBatchEV = p.MaxBatchEV
	}
	policy.DepositMultiplier = p.DepositMultiplier
	return policy, policy.Validate()
}

func (p Params) discoveryURLs() []*url.URL {
	urls := make([]*url.URL, len(p.Orchestrators))
	for i, item := range p.Orchestrators {
		urls[i] = item.Value
	}
	return urls
}

func (p Params) Validate() error {
	if err := validateDiscoveryPolling(p.DiscoveryRefreshInterval, p.DiscoveryMaxFailedRefreshes); err != nil {
		return err
	}
	if p.ChainRefreshInterval > 0 && p.ChainMaxAge > eth.MaxChainViewAge {
		return fmt.Errorf("--chain-max-age must be at most %s, far below the life of ticket parameters", eth.MaxChainViewAge)
	}
	if p.ChainRefreshInterval < 0 || (p.ChainRefreshInterval > 0 && p.ChainMaxAge <= p.ChainRefreshInterval) {
		return errors.New("--chain-refresh-interval must be nonnegative and --chain-max-age must be greater than it; an interval of 0 reads the chain directly")
	}
	if p.Network != "" && p.ChainID == nil {
		return errors.New("--network (or LIVEPEER_SIGNER_NETWORK) requires an explicit --chain-id and --controller-address for custom networks")
	}
	if p.KeystorePassword == nil && p.KeystorePasswordFile == "" {
		return errors.New("keystore password is required: set LIVEPEER_SIGNER_KEYSTORE_PASSWORD or supply --keystore-password-file (or LIVEPEER_SIGNER_KEYSTORE_PASSWORD_FILE)")
	}
	if p.Kafka != nil {
		if err := p.Kafka.Validate(); err != nil {
			return err
		}
	}
	if p.AuthWebhook != nil || len(p.AuthWebhookHeaders) > 0 {
		if err := validateAuthWebhook(p.AuthWebhook); err != nil {
			return errors.New("auth webhook URL (LIVEPEER_SIGNER_AUTH_WEBHOOK or --auth-webhook-file) must be an absolute HTTP or HTTPS URL without credentials or fragment")
		}
	}
	if _, err := p.AuthWebhookHeaders.MarshalText(); err != nil {
		return fmt.Errorf("invalid auth webhook headers (LIVEPEER_SIGNER_AUTH_WEBHOOK_HEADERS or --auth-webhook-headers-file): %w", err)
	}
	if p.OrchestratorWebhook != nil || len(p.OrchestratorWebhookHeaders) > 0 {
		if err := validateOrchestratorWebhook(p.OrchestratorWebhook, p.OrchestratorWebhookHeaders, p.OrchestratorWebhookRefreshInterval); err != nil {
			return err
		}
	}
	if _, err := p.payerPolicy(); err != nil {
		return fmt.Errorf("invalid payer limits (--max-ticket-ev, --max-batch-ev, --deposit-multiplier): %w", err)
	}
	if !p.MetricsListen.Addr().IsLoopback() {
		return errors.New("--metrics-listen (or LIVEPEER_SIGNER_METRICS_LISTEN) must bind a loopback IP:port address, e.g. 127.0.0.1:8938 or [::1]:8938")
	}
	if err := destination.ValidateURL(p.RPCURL); err != nil {
		return fmt.Errorf("invalid RPC URL (LIVEPEER_SIGNER_RPC_URL or --rpc-url-file): %w", err)
	}
	// A required, parseable address can still be empty.
	if p.Controller == (ethcommon.Address{}) || p.WeiPerUSD == nil && p.ETHUSDFeed == (ethcommon.Address{}) {
		return errors.New("--controller-address must be nonempty; supply a nonempty --eth-usd-feed or a positive --wei-per-usd conversion rate")
	}
	if _, err := newPricePolicy(p.MaxHourlyPrice, p.MaxFixedPrice); err != nil {
		return fmt.Errorf("invalid price limits (--max-hourly-price and --max-fixed-price): %w", err)
	}
	if p.WeiPerUSD != nil {
		if p.WeiPerUSD.Sign() <= 0 {
			return errors.New("--wei-per-usd must be positive")
		}
	} else if p.ETHUSDMaxAge <= 0 {
		return errors.New("--eth-usd-feed requires positive --eth-usd-max-age")
	}
	for _, endpoint := range p.discoveryURLs() {
		if err := discovery.ValidateURL(endpoint); err != nil {
			return errors.New("--orchestrators must contain absolute HTTP or HTTPS URLs without credentials, query or fragment")
		}
	}
	if _, err := destination.New("signer-discovery", p.DiscoveryGrants); err != nil {
		return fmt.Errorf("invalid --discovery-grants: %w", err)
	}
	return nil
}

func Serve(p Params) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return serve(ctx, p)
}

func serve(parent context.Context, p Params) error {
	if err := p.Validate(); err != nil {
		return err
	}
	ctx, stop := context.WithCancel(parent)
	defer stop()
	key, err := eth.OpenAccount(p.KeystoreDir(), p.KeystoreFile, p.KeystorePassword, p.KeystorePasswordFile, p.Account)
	if err != nil {
		return fmt.Errorf("open signer keystore (check --keystore-file or --data-dir and --account, and LIVEPEER_SIGNER_KEYSTORE_PASSWORD or --keystore-password-file): %w", err)
	}
	service := newService(key)
	defer service.Close()
	service.payerPolicy, _ = p.payerPolicy()
	rpc, err := eth.NewRPC(p.RPCURL, nil)
	if err != nil {
		return fmt.Errorf("configure signer RPC (LIVEPEER_SIGNER_RPC_URL or --rpc-url-file): %w", err)
	}
	defer rpc.Close()
	rpc.SetObserver(service.metrics.RPC)
	rpc.SetCallObserver(service.metrics.RPCCall)
	var chainID *big.Int
	if p.ChainID != nil {
		chainID = new(big.Int).SetUint64(*p.ChainID)
	}
	if err := rpc.CheckChainID(ctx, chainID); err != nil {
		return fmt.Errorf("verify signer RPC chain ID (check LIVEPEER_SIGNER_RPC_URL or --rpc-url-file, --chain-id and --network): %w", err)
	}
	contracts, err := eth.NewContracts(rpc, p.Controller)
	if err != nil {
		return fmt.Errorf("initialize signer contract bindings: %w", err)
	}
	var chainView *eth.ChainView
	service.SetPaymentChain(pm.EthereumChain{Client: eth.PaymentChain{Contracts: contracts}})
	if p.ChainRefreshInterval > 0 {
		chainView, err = eth.NewChainView(eth.PaymentChain{Contracts: contracts}, eth.ChainViewOptions{Interval: p.ChainRefreshInterval, MaxAge: p.ChainMaxAge,
			OnRefresh: service.metrics.ChainViewRefreshHook(slog.With("component", "signer")), OnBatchFallback: service.metrics.ChainViewBatchFallback})
		if err != nil {
			return fmt.Errorf("initialize chain view (check --chain-refresh-interval and --chain-max-age): %w", err)
		}
		service.metrics.ChainView(chainView.Age, chainView.Tracked)
		service.SetPaymentChain(chainView)
	}
	policy, _ := newPricePolicy(p.MaxHourlyPrice, p.MaxFixedPrice)
	policy.metrics = service.metrics
	if p.WeiPerUSD == nil {
		_ = policy.refreshFeed(ctx, func(ctx context.Context) (*big.Rat, time.Time, error) {
			return contracts.WeiPerUSD(ctx, p.ETHUSDFeed, p.ETHUSDMaxAge)
		})
	} else if err := policy.setRate(p.WeiPerUSD, time.Time{}); err != nil {
		return err
	}
	service.pricePolicy = policy
	if p.AuthWebhook != nil {
		if err := service.SetAuthWebhook(p.AuthWebhook, p.AuthWebhookHeaders); err != nil {
			return err
		}
	}
	if err := service.SetDiscoveryPolling(p.DiscoveryRefreshInterval, p.DiscoveryMaxFailedRefreshes); err != nil {
		return err
	}
	if err := service.SetDiscovery(p.discoveryURLs(), p.DiscoveryGrants...); err != nil {
		return err
	}
	if p.OrchestratorWebhook != nil {
		if err := service.SetOrchestratorWebhook(p.OrchestratorWebhook, p.OrchestratorWebhookHeaders, p.OrchestratorWebhookRefreshInterval); err != nil {
			return err
		}
	}
	producer, err := openKafkaProducer(ctx, p.Kafka, key.Address().Hex())
	if err != nil {
		return err
	}
	defer func() { _ = producer.Close() }()
	if producer != nil {
		service.metrics.Legacy(producer.collectMetrics)
		service.events = producer
	}
	listener, err := net.Listen("tcp", p.Listen.String())
	if err != nil {
		return fmt.Errorf("cannot start signer HTTP listener on %s; choose an available address with --listen or LIVEPEER_SIGNER_LISTEN: %w", p.Listen, err)
	}
	defer listener.Close()
	metricsListener, err := net.Listen("tcp", p.MetricsListen.String())
	if err != nil {
		return fmt.Errorf("cannot start signer metrics listener on %s; choose an available loopback address with --metrics-listen or LIVEPEER_SIGNER_METRICS_LISTEN: %w", p.MetricsListen, err)
	}
	defer metricsListener.Close()
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	metricsMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		funds, err := service.paymentChain.PayerFunds(ctx, key.Address(), ethcommon.Address{})
		if err == nil {
			service.observeFunds(funds, true)
		}
		if err != nil || pm.ValidatePayerFunds(funds) != nil || !service.pricePolicy.ready() || !producer.ready(ctx) {
			http.Error(w, "signer payment dependencies unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
	})
	metricsMux.Handle("GET /metrics", service.metrics.Handler())
	srv := &http.Server{BaseContext: func(net.Listener) context.Context { return ctx }, Handler: service, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, MaxHeaderValueCount: 128}
	metricsServer := &http.Server{BaseContext: srv.BaseContext, Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, MaxHeaderValueCount: 128}
	errCh := make(chan error, 2)
	var workers sync.WaitGroup
	// Keep delivery alive until in-flight HTTP handlers have finished enqueuing.
	publisherCtx, publisherStop := context.WithCancel(context.Background())
	defer publisherStop()
	if producer != nil {
		workers.Go(func() { producer.run(publisherCtx) })
	}
	if chainView != nil {
		workers.Go(func() { chainView.Run(ctx) })
	}
	workers.Go(func() { service.RunOrchestratorWebhook(ctx) })
	workers.Go(func() { service.RunDiscovery(ctx) })
	if p.WeiPerUSD == nil {
		workers.Go(func() {
			refreshPriceFeed(ctx, time.Hour, service.pricePolicy, func(readCtx context.Context) (*big.Rat, time.Time, error) {
				return contracts.WeiPerUSD(readCtx, p.ETHUSDFeed, p.ETHUSDMaxAge)
			})
		})
	}
	workers.Go(func() { errCh <- srv.Serve(listener) })
	workers.Go(func() { errCh <- metricsServer.Serve(metricsListener) })
	select {
	case <-ctx.Done():
	case err = <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := srv.Shutdown(shutdown); shutdownErr != nil {
		err = errors.Join(err, shutdownErr)
		_ = srv.Close()
	}
	if shutdownErr := metricsServer.Shutdown(shutdown); shutdownErr != nil {
		err = errors.Join(err, shutdownErr)
		_ = metricsServer.Close()
	}
	publisherStop()
	workers.Wait()
	return err
}

func Root(out, errOut io.Writer) *cobra.Command {
	params := new(Params)
	cmd := nodeconfig.Command("signer", nodeconfig.Mainnet, boa.Cmd[Params]{
		Use: "livepeer-signer", Short: "Standalone remote signer", Version: version.String(),
		Params: params, RejectUnknown: true,
		PreValidateFuncCtx: func(ctx *boa.HookContext, p *Params, _ *cobra.Command, _ []string) error {
			if p.Network == nodeconfig.Mainnet {
				nodeconfig.Default(ctx, &p.ChainID, new(uint64(42161)))
				nodeconfig.Default(ctx, &p.Controller, nodeconfig.Controller)
				if p.WeiPerUSD == nil {
					nodeconfig.Default(ctx, &p.ETHUSDFeed, nodeconfig.ETHUSDFeed)
				}
			}
			return nil
		},
		Args: cobra.NoArgs,
		RunFuncE: func(p *Params, _ *cobra.Command, _ []string) error {
			return Serve(*p)
		},
	}, &params.ConfigFile).ToCobra()
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	type storageParams struct {
		ConfigFile string `persistent:"true" name:"config" configfile:"optional-default" basepath:"source" default:"config.toml" boa:"noconfig"`
		nodeconfig.PersistentSettings
		Kafka struct {
			OutboxDB string `persistent:"true" file:"optional" default:"events.sqlite"`
		}
	}
	storage := new(storageParams)
	migrate := migrations.Command(nodeconfig.Command("signer", nodeconfig.Mainnet, boa.Cmd[storageParams]{Params: storage}, &storage.ConfigFile), &storage.Kafka.OutboxDB, outboxMigrationFiles, openOutboxDB)
	cmd.AddCommand(migrate)
	cmd.InitDefaultCompletionCmd()
	return cmd
}
