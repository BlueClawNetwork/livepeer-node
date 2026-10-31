package orchestrator

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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/cmd/version"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/nodeconfig"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/livepeer/node/pm"
	"github.com/spf13/cobra"
)

type Params struct {
	ConfigFile string `name:"config" configfile:"optional-default" basepath:"source" default:"config.toml" boa:"noconfig" descr:"Configuration file; empty disables discovery"`
	nodeconfig.Settings
	Account                   *ethcommon.Address `descr:"Keystore account address"`
	OrchestratorAddress       *ethcommon.Address `descr:"Registered orchestrator address receiving ticket payments; defaults to the keystore account"`
	ETHUSDFeed                *ethcommon.Address `name:"eth-usd-feed" descr:"ETH/USD oracle address, alternative to a fixed wei-per-usd rate"`
	PriceMaxAge               time.Duration      `default:"2h" descr:"Maximum age of the oracle observation"`
	Listen                    netip.AddrPort     `default:"127.0.0.1:8935" descr:"Public HTTP listener"`
	MetricsListen             netip.AddrPort     `default:"127.0.0.1:8936" descr:"Loopback metrics listener"`
	ServiceURL                boa.Text[*url.URL] `optional:"true" descr:"Public orchestrator base URL; defaults to http://listen"`
	RunnerServiceURL          boa.Text[*url.URL] `optional:"true" descr:"Runner-facing base URL for callbacks and trickle; defaults to service-url"`
	ProxyURLTemplate          string             `optional:"true" descr:"Generated proxy URL with {proxy} in a hostname label or final path segment"`
	BootstrapSecret           string             `secret:"true" optional:"true" descr:"Dynamic runner bootstrap credential"`
	BootstrapSecretFile       string             `basepath:"source" secretfor:"BootstrapSecret" descr:"File containing runner bootstrap credential"`
	RunnerConfig              string             `basepath:"source" optional:"true" file:"true" descr:"Static runner TOML path"`
	RedeemerDB                string             `optional:"true" file:"optional"`
	KeystoreFile              string             `basepath:"source" optional:"true" descr:"Encrypted geth redemption account JSON file"`
	KeystorePassword          *string            `secret:"true" optional:"true" descr:"Keystore decryption password"`
	KeystorePasswordFile      string             `basepath:"source" secretfor:"KeystorePassword" descr:"Owner-only file containing the exact keystore password bytes"`
	RPCURL                    *url.URL           `name:"rpc-url" secret:"true" optional:"true"`
	RPCURLFile                string             `basepath:"source" name:"rpc-url-file" secretfor:"RPCURL"`
	ChainID                   *uint64            `min:"1"`
	RedeemerTxTimeout         time.Duration      `default:"5m" descr:"Inclusion timeout for each redemption transaction attempt"`
	RedeemerMaxTxReplacements int                `default:"1" min:"0" descr:"Maximum fee replacements per redemption attempt"`
	RedeemerMaxFeePerGas      *uint256.Int       `descr:"Optional maximum redemption fee in wei per gas"`
	Controller                *ethcommon.Address `name:"controller-address"`
	WeiPerUSD                 *big.Rat
	TicketFaceValue           *uint256.Int
	TicketWinProb             *uint256.Int
	RunnerGrants              []string      `optional:"true" descr:"Private runner IP[:port] or CIDR grants"`
	SessionProxyGrants        []string      `optional:"true" descr:"Private session proxy IP[:port] or CIDR grants"`
	HealthGrants              []string      `optional:"true" descr:"Private static runner health IP[:port] or CIDR grants"`
	HeartbeatInterval         time.Duration `default:"5s" descr:"Runner heartbeat interval"`
	HeartbeatTTL              time.Duration `default:"30s" descr:"Runner heartbeat expiry"`
	ChainRefreshInterval      time.Duration `default:"5s" descr:"Chain view refresh interval for paid requests; 0 reads the chain on every request"`
	ChainMaxAge               time.Duration `default:"30s" descr:"Refuse paid requests when the chain view is older than this; must exceed chain-refresh-interval"`
	PrintConfig               bool          `optional:"true" boa:"noconfig,noenv" descr:"Print audited redacted TOML configuration"`
}

func (p Params) paymentsRequested() bool {
	return p.Account != nil || p.OrchestratorAddress != nil || p.KeystoreFile != "" || p.KeystorePassword != nil || p.KeystorePasswordFile != "" || p.RedeemerDB != "" || p.RPCURL != nil || p.RPCURLFile != "" || p.ChainID != nil || p.Controller != nil || p.WeiPerUSD != nil || p.ETHUSDFeed != nil || p.TicketFaceValue != nil || p.TicketWinProb != nil || p.RedeemerMaxFeePerGas != nil
}

func (p Params) Validate() error {
	if p.BootstrapSecret == "" && p.RunnerConfig == "" {
		return errors.New("bootstrap secret or static runner config is required: set LIVEPEER_ORCHESTRATOR_BOOTSTRAP_SECRET, supply --bootstrap-secret-file (or LIVEPEER_ORCHESTRATOR_BOOTSTRAP_SECRET_FILE), or supply --runner-config (or LIVEPEER_ORCHESTRATOR_RUNNER_CONFIG) with a static runner TOML file")
	}
	if p.paymentsRequested() {
		if p.Network == "offchain" {
			return errors.New("on-chain payments require an explicit on-chain --network (or LIVEPEER_ORCHESTRATOR_NETWORK), e.g. '--network arbitrum-one-mainnet'")
		}
		if p.OrchestratorAddress != nil && *p.OrchestratorAddress == (ethcommon.Address{}) {
			return errors.New("--orchestrator-address must be a nonempty Ethereum address")
		}
		if p.KeystorePassword == nil && p.KeystorePasswordFile == "" || p.RedeemerDB == "" || p.RPCURL == nil || p.ChainID == nil || p.Controller == nil || (p.WeiPerUSD == nil && p.ETHUSDFeed == nil) || p.TicketFaceValue == nil || p.TicketWinProb == nil {
			return errors.New("on-chain payment requires --redeemer-db, LIVEPEER_ORCHESTRATOR_KEYSTORE_PASSWORD or --keystore-password-file, LIVEPEER_ORCHESTRATOR_RPC_URL or --rpc-url-file, --chain-id, --controller-address, --wei-per-usd or --eth-usd-feed, --ticket-face-value and --ticket-win-prob")
		}
		if *p.Controller == (ethcommon.Address{}) {
			return errors.New("--controller-address must be a nonempty Ethereum address")
		}
		if *p.ChainID == 0 || p.TicketFaceValue.IsZero() || p.TicketWinProb.IsZero() {
			return errors.New("--chain-id, --ticket-face-value and --ticket-win-prob must be positive")
		}
		if p.TicketWinProb.Eq(new(uint256.Int).SetAllOne()) {
			return errors.New("--ticket-win-prob must be less than 2^256 - 1")
		}
		if p.RedeemerTxTimeout <= 0 || p.RedeemerMaxTxReplacements < 0 {
			return errors.New("--redeemer-tx-timeout must be positive and --redeemer-max-tx-replacements must be nonnegative")
		}
		if p.RedeemerMaxFeePerGas != nil && p.RedeemerMaxFeePerGas.IsZero() {
			return errors.New("--redeemer-max-fee-per-gas must be positive")
		}
		if p.ETHUSDFeed != nil {
			if p.WeiPerUSD != nil || *p.ETHUSDFeed == (ethcommon.Address{}) || p.PriceMaxAge <= 0 {
				return errors.New("--eth-usd-feed requires a nonempty Ethereum address, positive --price-max-age and no fixed --wei-per-usd")
			}
		} else if p.WeiPerUSD.Sign() <= 0 {
			return errors.New("--wei-per-usd must be positive")
		}
		if err := destination.ValidateURL(p.RPCURL); err != nil {
			return fmt.Errorf("invalid payment RPC URL (LIVEPEER_ORCHESTRATOR_RPC_URL or --rpc-url-file): %w", err)
		}
	}
	if err := validateProxyTemplate(p.ProxyURLTemplate); err != nil {
		return fmt.Errorf("invalid --proxy-url-template: %w", err)
	}
	if !p.Listen.IsValid() {
		return errors.New("--listen (or LIVEPEER_ORCHESTRATOR_LISTEN) must be an IP:port address, e.g. 127.0.0.1:8935 or [::1]:8935")
	}
	if !p.MetricsListen.Addr().IsLoopback() {
		return errors.New("--metrics-listen (or LIVEPEER_ORCHESTRATOR_METRICS_LISTEN) must bind a loopback IP:port address, e.g. 127.0.0.1:8936 or [::1]:8936")
	}
	for _, endpoint := range []struct {
		name string
		url  *url.URL
	}{{"service-url", p.ServiceURL.Value}, {"runner-service-url", p.RunnerServiceURL.Value}} {
		if endpoint.name == "runner-service-url" && endpoint.url == nil {
			continue
		}
		if u := endpoint.url; destination.ValidateURL(u) != nil || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return fmt.Errorf("--%s must be an absolute HTTP or HTTPS URL without credentials, query or fragment", endpoint.name)
		}
	}
	if p.HeartbeatInterval <= 0 || p.HeartbeatTTL <= p.HeartbeatInterval {
		return errors.New("--heartbeat-interval must be positive and --heartbeat-ttl must be greater than --heartbeat-interval")
	}
	if p.ChainRefreshInterval < 0 || (p.ChainRefreshInterval > 0 && p.ChainMaxAge <= p.ChainRefreshInterval) {
		return errors.New("--chain-refresh-interval must be nonnegative and --chain-max-age must be greater than it; an interval of 0 reads the chain directly")
	}
	for _, item := range []struct {
		purpose string
		grants  []string
	}{{"--runner-grants", p.RunnerGrants}, {"--session-proxy-grants", p.SessionProxyGrants}, {"--health-grants", p.HealthGrants}} {
		if _, err := destination.New(item.purpose, item.grants); err != nil {
			return err
		}
	}
	return nil
}

func Root(out, errOut io.Writer) *cobra.Command {
	params := new(Params)
	cmd := nodeconfig.Command("orchestrator", "offchain", boa.Cmd[Params]{
		Use: "livepeer-orchestrator", Short: "Standalone Live Runner orchestrator", Version: version.String(),
		Params: params, RejectUnknown: true,
		PostConfigFuncCtx: func(ctx *boa.HookContext, p *Params, _ *cobra.Command, _ []string) error {
			nodeconfig.Default(ctx, &p.ServiceURL, boa.Text[*url.URL]{Value: &url.URL{Scheme: "http", Host: p.Listen.String()}})
			if p.paymentsRequested() {
				if p.Network == nodeconfig.Mainnet {
					nodeconfig.Default(ctx, &p.ChainID, new(uint64(42161)))
					nodeconfig.Default(ctx, &p.Controller, new(nodeconfig.Controller))
					if p.WeiPerUSD == nil {
						nodeconfig.Default(ctx, &p.ETHUSDFeed, new(nodeconfig.ETHUSDFeed))
					}
				}
				nodeconfig.Default(ctx, &p.RedeemerDB, "payments.sqlite")
			}
			return nil
		},
		Args: cobra.NoArgs,
		RunFuncCtxE: func(ctx *boa.HookContext, p *Params, cmd *cobra.Command, _ []string) error {
			if p.PrintConfig {
				return printConfig(ctx, p, cmd.OutOrStdout())
			}
			if err := p.Validate(); err != nil {
				return boa.NewUserInputError(err)
			}
			return Serve(cmd.Context(), *p, cmd.ErrOrStderr())
		},
	}, &params.ConfigFile).ToCobra()
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	type storageParams struct {
		ConfigFile string `persistent:"true" name:"config" configfile:"optional-default" basepath:"source" default:"config.toml" boa:"noconfig"`
		nodeconfig.PersistentSettings
		RedeemerDB string `persistent:"true" file:"optional" default:"payments.sqlite"`
	}
	storage := new(storageParams)
	migrate := migrations.Command(nodeconfig.Command("orchestrator", nodeconfig.Mainnet, boa.Cmd[storageParams]{Params: storage}, &storage.ConfigFile), &storage.RedeemerDB, redeemerMigrationFiles, openRedeemerDB)
	cmd.AddCommand(redemptionCommand(), migrate)
	cmd.InitDefaultCompletionCmd()
	return cmd
}

func printConfig(ctx *boa.HookContext, p *Params, out io.Writer) error {
	// Only audited fields are printed; secrets and sensitive paths stay omitted.
	values := map[string]any{
		"Network": &p.Network, "Account": &p.Account, "OrchestratorAddress": &p.OrchestratorAddress,
		"Listen": &p.Listen, "MetricsListen": &p.MetricsListen,
		"RunnerGrants": &p.RunnerGrants, "SessionProxyGrants": &p.SessionProxyGrants, "HealthGrants": &p.HealthGrants,
		"HeartbeatInterval": &p.HeartbeatInterval, "HeartbeatTTL": &p.HeartbeatTTL,
		"RedeemerTxTimeout": &p.RedeemerTxTimeout, "RedeemerMaxTxReplacements": &p.RedeemerMaxTxReplacements,
		"RedeemerDB": &p.RedeemerDB, "ChainID": &p.ChainID, "RedeemerMaxFeePerGas": &p.RedeemerMaxFeePerGas,
		"Controller": &p.Controller, "WeiPerUSD": &p.WeiPerUSD, "ETHUSDFeed": &p.ETHUSDFeed,
		"PriceMaxAge": &p.PriceMaxAge, "TicketFaceValue": &p.TicketFaceValue, "TicketWinProb": &p.TicketWinProb,
		"ChainRefreshInterval": &p.ChainRefreshInterval, "ChainMaxAge": &p.ChainMaxAge,
	}
	for name, ptr := range values {
		if !ctx.HasValue(ptr) {
			delete(values, name)
		}
	}
	data, err := toml.Marshal(values)
	if err != nil {
		return err
	}
	_, err = out.Write(data)
	return err
}

func loadStatic(path string, registry *Registry) error {
	if path == "" {
		return nil
	}
	var config struct {
		Runners []StaticRunner
	}
	meta, err := toml.DecodeFile(path, &config)
	if err != nil {
		return err
	}
	if len(meta.Undecoded()) > 0 {
		return fmt.Errorf("unknown static runner keys: %v", meta.Undecoded())
	}
	for _, item := range config.Runners {
		if err := registry.AddStatic(item); err != nil {
			return err
		}
	}
	return nil
}

func Serve(parent context.Context, p Params, logOut io.Writer) (result error) {
	if err := p.Validate(); err != nil {
		return err
	}
	runnerPolicy, err := destination.New("runner", p.RunnerGrants)
	if err != nil {
		return err
	}
	proxyPolicy, err := destination.New("session-proxy", p.SessionProxyGrants)
	if err != nil {
		return err
	}
	healthPolicy, err := destination.New("static-runner-health", p.HealthGrants)
	if err != nil {
		return err
	}
	registry := NewRegistry(p.BootstrapSecret, p.ServiceURL.String(), p.HeartbeatInterval, p.HeartbeatTTL)
	registry.runnerService = strings.TrimRight(p.RunnerServiceURL.String(), "/")
	logger := slog.New(slog.NewTextHandler(logOut, nil))
	var engine *PaymentEngine
	var redeemerDB *RedeemerDB
	var paymentChain eth.PaymentChain
	var chainView *eth.ChainView
	var redeemerKey *eth.Key
	var paymentChainID *big.Int
	if p.paymentsRequested() {
		registry.SetWeiPerUSD(p.WeiPerUSD)
		redeemerKey, err = eth.OpenAccount(p.KeystoreDir(), p.KeystoreFile, p.KeystorePassword, p.KeystorePasswordFile, p.Account)
		if err != nil {
			return fmt.Errorf("open payment keystore (check --keystore-file or --data-dir and --account, and LIVEPEER_ORCHESTRATOR_KEYSTORE_PASSWORD or --keystore-password-file): %w", err)
		}
		rpc, err := eth.NewRPC(p.RPCURL, nil)
		if err != nil {
			return fmt.Errorf("configure payment RPC (LIVEPEER_ORCHESTRATOR_RPC_URL or --rpc-url-file): %w", err)
		}
		defer rpc.Close()
		rpc.SetObserver(registry.metrics.RPC)
		rpc.SetCallObserver(registry.metrics.RPCCall)
		paymentChainID = new(big.Int).SetUint64(*p.ChainID)
		if err := rpc.CheckChainID(parent, paymentChainID); err != nil {
			return fmt.Errorf("verify payment RPC chain ID (check LIVEPEER_ORCHESTRATOR_RPC_URL or --rpc-url-file, --chain-id and --network): %w", err)
		}
		redeemerDB, err = OpenRedeemerDB(p.RedeemerDB)
		if err != nil {
			return fmt.Errorf("open payment database %q (--redeemer-db or LIVEPEER_ORCHESTRATOR_REDEEMER_DB): %w", p.RedeemerDB, err)
		}
		defer redeemerDB.Close()
		contracts, err := eth.NewContracts(rpc, *p.Controller)
		if err != nil {
			return fmt.Errorf("initialize payment contract bindings: %w", err)
		}
		if p.RedeemerMaxFeePerGas != nil {
			contracts.MaxFeePerGas = p.RedeemerMaxFeePerGas.ToBig()
		}
		paymentChain = eth.PaymentChain{Contracts: contracts}
		if p.ETHUSDFeed != nil {
			rate, until, err := contracts.WeiPerUSD(parent, *p.ETHUSDFeed, p.PriceMaxAge)
			if err != nil {
				registry.metrics.Add("price_feed_updates_total", 1, observability.Labels{"outcome": "error"})
				return fmt.Errorf("read ETH/USD price feed (check --eth-usd-feed, --price-max-age, and LIVEPEER_ORCHESTRATOR_RPC_URL or --rpc-url-file): %w", err)
			}
			registry.setRate(rate, until)
			registry.metrics.Add("price_feed_updates_total", 1, observability.Labels{"outcome": "success"})
		}
		recipient := redeemerKey.Address()
		if p.OrchestratorAddress != nil {
			recipient = *p.OrchestratorAddress
		}
		var engineChain pm.PaymentChain = pm.EthereumChain{Client: paymentChain}
		if p.ChainRefreshInterval > 0 {
			chainView, err = eth.NewChainView(paymentChain, eth.ChainViewOptions{Interval: p.ChainRefreshInterval, MaxAge: p.ChainMaxAge, Recipients: []ethcommon.Address{recipient},
				OnRefresh: registry.metrics.ChainViewRefreshHook(logger.With("component", "orchestrator")), OnBatchFallback: registry.metrics.ChainViewBatchFallback})
			if err != nil {
				return fmt.Errorf("initialize chain view (check --chain-refresh-interval and --chain-max-age): %w", err)
			}
			registry.metrics.ChainView(chainView.Age, chainView.Tracked)
			engineChain = chainView
		}
		engine, err = NewPaymentEngine(redeemerDB, engineChain, recipient, p.TicketFaceValue.ToBig(), p.TicketWinProb.ToBig())
		if err != nil {
			return fmt.Errorf("initialize payment engine (check --account, --ticket-face-value and --ticket-win-prob): %w", err)
		}
	}
	registry.proxyTemplate = p.ProxyURLTemplate
	registry.logger = logger.With("component", "orchestrator")
	if err := loadStatic(p.RunnerConfig, registry); err != nil {
		return fmt.Errorf("load static runners from %q (--runner-config or LIVEPEER_ORCHESTRATOR_RUNNER_CONFIG): %w", p.RunnerConfig, err)
	}
	app := NewServer(registry, runnerPolicy, proxyPolicy, logger)
	logger = app.logger
	defer app.Close()
	app.SetPayment(engine)
	readiness := &paidReadiness{interval: p.HeartbeatInterval}
	if chainView != nil {
		readiness.view = chainView
	}
	if engine != nil {
		registry.metrics.Gauge("redemption_last_success_timestamp_seconds", nil).Set(0)
		registry.metrics.Gauge("redemption_worker_last_completed_timestamp_seconds", nil).Set(0)
		readiness.account = redeemerKey.Address()
		redeemerDB.readiness = readiness
	}
	mainListener, err := net.Listen("tcp", p.Listen.String())
	if err != nil {
		return fmt.Errorf("cannot start orchestrator HTTP listener on %s; choose an available address with --listen or LIVEPEER_ORCHESTRATOR_LISTEN: %w", p.Listen, err)
	}
	defer mainListener.Close()
	metricsListener, err := net.Listen("tcp", p.MetricsListen.String())
	if err != nil {
		return fmt.Errorf("cannot start orchestrator metrics listener on %s; choose an available loopback address with --metrics-listen or LIVEPEER_ORCHESTRATOR_METRICS_LISTEN: %w", p.MetricsListen, err)
	}
	defer metricsListener.Close()
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	metricsMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if engine != nil && !readiness.ready(registry) {
			http.Error(w, "orchestrator payment dependencies unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
	})
	metricsMux.Handle("GET /metrics", registry.metrics.Handler())
	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if engine != nil {
		if err := StartRedemptionRecovery(ctx, redeemerDB, paymentChain, redeemerKey, RedemptionOptions{Timeout: p.RedeemerTxTimeout, MaxReplacements: p.RedeemerMaxTxReplacements}); err != nil {
			return err
		}
	}
	mainServer := &http.Server{BaseContext: func(net.Listener) context.Context { return ctx }, Handler: app, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 1 << 20, MaxHeaderValueCount: 128}
	metricsServer := &http.Server{Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 1 << 16, MaxHeaderValueCount: 128}
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Go(func() { errs <- mainServer.Serve(mainListener) })
	workers.Go(func() { errs <- metricsServer.Serve(metricsListener) })
	logger.Info("orchestrator started", "event", "started", "operation", "serve", "outcome", "success", "listen", p.Listen, "metrics_listen", p.MetricsListen)
	// Independent bounded workers keep slow RPC/health calls off the billing
	// and expiry paths. Every worker uses the shutdown context and is joined.
	startWorker := func(period, timeout time.Duration, work func(context.Context)) {
		workers.Go(func() {
			ticker := time.NewTicker(period)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					tickCtx, done := context.WithTimeout(ctx, timeout)
					work(tickCtx)
					done()
				}
			}
		})
	}
	startWorker(p.HeartbeatInterval, time.Second, func(context.Context) { registry.Expire() })
	workers.Go(func() { app.runO2RKeepalives(ctx, 10*time.Second) })
	if chainView != nil {
		workers.Go(func() { chainView.Run(ctx) })
	}
	if engine != nil {
		startWorker(p.HeartbeatInterval, 30*time.Second, func(ctx context.Context) {
			readiness.check(ctx, redeemerDB, paymentChain, engine, registry.metrics, logger)
		})
		if p.ETHUSDFeed != nil {
			startWorker(time.Hour, 30*time.Second, func(ctx context.Context) {
				rate, until, err := paymentChain.Contracts.WeiPerUSD(ctx, *p.ETHUSDFeed, p.PriceMaxAge)
				if err != nil {
					registry.metrics.Add("price_feed_updates_total", 1, observability.Labels{"outcome": "error"})
					logger.Error("price feed unavailable", "event", "price_feed", "operation", "refresh", "outcome", "error", "error_kind", observability.ErrorKind(err))
					return
				}
				registry.setRate(rate, until)
				registry.metrics.Add("price_feed_updates_total", 1, observability.Labels{"outcome": "success"})
			})
		}
		startWorker(time.Second, 5*time.Second, app.ChargePaidSessions)
		startWorker(time.Minute, 5*time.Second, func(ctx context.Context) {
			if err := engine.PruneControlState(ctx); err != nil {
				logger.Error("payment retention", "event", "payment_retention", "operation", "prune", "outcome", "error", "error_kind", observability.ErrorKind(err))
			}
		})
		startWorker(p.HeartbeatInterval, 30*time.Second, func(ctx context.Context) {
			failures := ProcessRedemptions(ctx, redeemerDB, paymentChain, redeemerKey, paymentChainID)
			for _, err := range failures {
				registry.metrics.Add("redemption_errors_total", 1, observability.Labels{"error_kind": observability.ErrorKind(err)})
				logger.Error("redemption failed", "event", "redemption_pass", "operation", "redeem", "outcome", "error", "error_kind", observability.ErrorKind(err))
			}
			readiness.mu.Lock()
			readiness.workerCompleted = time.Now()
			readiness.mu.Unlock()
			registry.metrics.Gauge("redemption_worker_last_completed_timestamp_seconds", nil).Set(float64(time.Now().Unix()))
			if len(failures) == 0 {
				registry.metrics.Gauge("redemption_last_success_timestamp_seconds", nil).Set(float64(time.Now().Unix()))
			}
		})
	}
	client := healthPolicy.Client()
	client.Timeout = 5 * time.Second
	defer client.CloseIdleConnections()
	if p.RunnerConfig != "" {
		startWorker(p.HeartbeatInterval, 30*time.Second, func(ctx context.Context) { registry.CheckStaticHealth(ctx, client) })
	}
	select {
	case <-ctx.Done():
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	cancel()
	app.Close()
	shutdownCtx, done := context.WithTimeout(context.Background(), 15*time.Second)
	defer done()
	if err := mainServer.Shutdown(shutdownCtx); err != nil {
		result = errors.Join(result, err)
		_ = mainServer.Close()
	}
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		result = errors.Join(result, err)
		_ = metricsServer.Close()
	}
	workers.Wait()
	return result
}
