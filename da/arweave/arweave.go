package arweave

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/everFinance/goar"
	goartypes "github.com/everFinance/goar/types"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"

	"github.com/rollkit/rollkit/da"
	"github.com/rollkit/rollkit/log"
	"github.com/rollkit/rollkit/types"
	pb "github.com/rollkit/rollkit/types/pb/rollkit"
)

// DataAvailabilityLayerClient use celestia-node public API.
type DataAvailabilityLayerClient struct {
	client *goar.Wallet

	namespaceID types.NamespaceID // Glacier NetworkID Tag
	config      Config
	logger      log.Logger

	// trace the Arweave Tx and Height for test!
	daTxBlockTracer map[string]int // map[tx]height
}

var _ da.DataAvailabilityLayerClient = &DataAvailabilityLayerClient{}
var _ da.BlockRetriever = &DataAvailabilityLayerClient{}

// Config stores Celestia DALC configuration parameters.
type Config struct {
	ARNode         string          `json:"ar_node"`
	ARWalletBase64 string          `json:"ar_wallet_base64"` // wallet base64
	Timeout        time.Duration   `json:"timeout"`
	ARTags         []goartypes.Tag `json:"ar_tags"`
}

// 	wallet, err := goar.NewWalletFromPath(walletPath, arNodeEndpoint)

// Init initializes DataAvailabilityLayerClient instance.
func (c *DataAvailabilityLayerClient) Init(namespaceID types.NamespaceID, config []byte, kvStore ds.Datastore, logger log.Logger) error {
	c.namespaceID = namespaceID
	c.logger = logger
	c.daTxBlockTracer = map[string]int{}
	if len(config) > 0 {
		return json.Unmarshal(config, &c.config)
	}

	return nil
}

// Start prepares DataAvailabilityLayerClient to work.
func (c *DataAvailabilityLayerClient) Start() error {
	c.logger.Info("starting Celestia Data Availability Layer Client", "baseURL", c.config.ARNode)
	var err error
	wallet, err := base64.RawStdEncoding.DecodeString(c.config.ARWalletBase64)
	if err != nil {
		return err
	}
	c.client, err = goar.NewWallet(wallet, c.config.ARNode)
	return err
}

// Stop stops DataAvailabilityLayerClient.
func (c *DataAvailabilityLayerClient) Stop() error {
	c.logger.Info("stopping Celestia Data Availability Layer Client")
	return nil
}

// SubmitBlock submits a block to DA layer.
func (c *DataAvailabilityLayerClient) SubmitBlock(ctx context.Context, block *types.Block) da.ResultSubmitBlock {
	blob, err := block.MarshalBinary()
	if err != nil {
		return da.ResultSubmitBlock{
			BaseResult: da.BaseResult{
				Code:    da.StatusError,
				Message: err.Error(),
			},
		}
	}

	txResponse, err := c.client.SendData(blob, c.config.ARTags)
	if err != nil {
		return da.ResultSubmitBlock{
			BaseResult: da.BaseResult{
				Code:    da.StatusError,
				Message: err.Error(),
			},
		}
	}

	if txResponse.ID == "" {
		return da.ResultSubmitBlock{
			BaseResult: da.BaseResult{
				Code:    da.StatusError,
				Message: "Codespace: arweave, Message: no txId response",
			},
		}
	}

	for {
		status, err := c.client.Client.GetTransactionStatus(txResponse.ID)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		if status.BlockHeight != 0 {
			c.daTxBlockTracer[txResponse.ID] = status.BlockHeight
			break
		}
	}

	return da.ResultSubmitBlock{
		BaseResult: da.BaseResult{
			Code:     da.StatusSuccess,
			Message:  "tx hash: " + txResponse.ID,
			DAHeight: uint64(c.daTxBlockTracer[txResponse.ID]),
		},
	}
}

// CheckBlockAvailability queries DA layer to check data availability of block at given height.
func (c *DataAvailabilityLayerClient) CheckBlockAvailability(ctx context.Context, dataLayerHeight uint64) da.ResultCheckBlock {
	txs := []string{}
	for tx, height := range c.daTxBlockTracer {
		if dataLayerHeight == uint64(height) {
			txs = append(txs, tx)
		}
	}
	if len(txs) == 0 {
		return da.ResultCheckBlock{
			BaseResult: da.BaseResult{
				Code:    da.StatusError,
				Message: "empty block",
			},
		}
	}

	shares := []int{}
	for _, tx := range txs {
		status, err := c.client.Client.GetTransactionStatus(tx)
		if err != nil {
			return da.ResultCheckBlock{
				BaseResult: da.BaseResult{
					Code:    da.StatusError,
					Message: err.Error(),
				},
			}
		}
		if status.BlockHeight != 0 {
			shares = append(shares, status.BlockHeight)
		}
	}

	return da.ResultCheckBlock{
		BaseResult: da.BaseResult{
			Code:     da.StatusSuccess,
			DAHeight: dataLayerHeight,
		},
		DataAvailable: len(txs) == len(shares),
	}
}

// RetrieveBlocks gets a batch of blocks from DA layer.
func (c *DataAvailabilityLayerClient) RetrieveBlocks(ctx context.Context, dataLayerHeight uint64) da.ResultRetrieveBlocks {
	txs := []string{}
	for tx, height := range c.daTxBlockTracer {
		if dataLayerHeight == uint64(height) {
			txs = append(txs, tx)
		}
	}
	if len(txs) == 0 {
		return da.ResultRetrieveBlocks{
			BaseResult: da.BaseResult{
				Code:    da.StatusError,
				Message: "empty block",
			},
		}
	}

	blocks := make([]*types.Block, len(txs))
	for i, tx := range txs {
		msg, err := c.client.Client.GetTransactionDataByGateway(tx)
		if err != nil {
			return da.ResultRetrieveBlocks{
				BaseResult: da.BaseResult{
					Code:    da.StatusError,
					Message: err.Error(),
				},
			}
		}
		var block pb.Block
		err = proto.Unmarshal(msg, &block)
		if err != nil {
			c.logger.Error("failed to unmarshal block", "daHeight", dataLayerHeight, "position", i, "error", err)
			continue
		}
		blocks[i] = new(types.Block)
		err = blocks[i].FromProto(&block)
		if err != nil {
			return da.ResultRetrieveBlocks{
				BaseResult: da.BaseResult{
					Code:    da.StatusError,
					Message: err.Error(),
				},
			}
		}
	}

	return da.ResultRetrieveBlocks{
		BaseResult: da.BaseResult{
			Code:     da.StatusSuccess,
			DAHeight: dataLayerHeight,
		},
		Blocks: blocks,
	}
}
