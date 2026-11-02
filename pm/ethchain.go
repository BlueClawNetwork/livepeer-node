package pm

import (
	"context"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
)

type ChainSnapshot = eth.ChainSnapshot

// PayerChain contains the payer collateral observation used by both payment sides.
type PayerChain interface {
	PayerFunds(context.Context, ethcommon.Address, ethcommon.Address) (PayerFunds, error)
}

// PaymentChain contains only the Ethereum reads needed by payment receipt.
type PaymentChain interface {
	Snapshot(context.Context) (ChainSnapshot, error)
	IsActiveAt(context.Context, ethcommon.Address, ChainSnapshot) (bool, error)
	PayerChain
}

type PayerFunds = eth.SenderInfo

// EthereumChain adapts retained Ethereum contract reads to the payment
// engine's interface. Contract ABI and RPC logic remain in eth.
type EthereumChain struct{ Client eth.PaymentChain }

func (c EthereumChain) Snapshot(ctx context.Context) (ChainSnapshot, error) {
	state, err := c.Client.Snapshot(ctx)
	if err != nil {
		return ChainSnapshot{}, err
	}
	return state, nil
}

func (c EthereumChain) PayerFunds(ctx context.Context, payer, recipient ethcommon.Address) (PayerFunds, error) {
	return c.Client.SenderInfo(ctx, payer, recipient)
}

func (c EthereumChain) IsActiveAt(ctx context.Context, recipient ethcommon.Address, snapshot ChainSnapshot) (bool, error) {
	return c.Client.IsActiveAt(ctx, recipient, snapshot)
}

var _ PaymentChain = EthereumChain{}
var _ PaymentChain = (*eth.ChainView)(nil)
