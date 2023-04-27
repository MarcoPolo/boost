package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/storage/sealer/partialfile"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/ipfs/go-cidutil/cidenc"
	"github.com/multiformats/go-multibase"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var disasterRecoveryCmd = &cli.Command{
	Name:  "disaster-recovery",
	Usage: "Disaster Recovery commands",
	Subcommands: []*cli.Command{
		restorePieceStoreCmd,
	},
}

var restorePieceStoreCmd = &cli.Command{
	Name:   "restore-piece-store", // TODO: Rename to "restore-lid" or something like that maybe?
	Usage:  "Restore Piece store",
	Before: before,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		// Connect to the storage API and create a sector accessor
		storageApiInfo := cctx.String("api-storage")
		sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
		if err != nil {
			return err
		}
		defer storageCloser()

		_ = sa

		maddr, err := getActorAddress(ctx, cctx)
		if err != nil {
			return err
		}

		sectors, err := fullnodeApi.StateMinerSectors(ctx, maddr, nil, types.EmptyTSK)
		if err != nil {
			return err
		}

		for _, info := range sectors {
			if len(info.DealIDs) > 2 {
				fmt.Println("sector number: ", info.SectorNumber, "; deals: ", info.DealIDs)

				for _, did := range info.DealIDs {
					marketDeal, err := fullnodeApi.StateMarketStorageDeal(ctx, did, types.EmptyTSK)
					if err != nil {
						return err
					}

					l := "(not a string)"
					if marketDeal.Proposal.Label.IsString() {
						l, err = marketDeal.Proposal.Label.ToString()
						if err != nil {
							return err
						}
					}

					fmt.Println("piece cid: ", marketDeal.Proposal.PieceCID, "; piece size: ", marketDeal.Proposal.PieceSize, "; label: ", l)
				}

				break
			}
		}

		//path := "/Users/nonsense/s-t01953925-28" // unsealed file
		path := "/Users/nonsense/s-t01000-1" // unsealed file

		ss8MiB := 8 << 20 // 8MiB sector
		_ = ss8MiB

		ss32GiB := 32 << 30 // 32GiB sector
		_ = ss32GiB

		//maxPieceSize := abi.PaddedPieceSize(ss32GiB)
		maxPieceSize := abi.PaddedPieceSize(ss8MiB)

		pf, err := partialfile.OpenPartialFile(maxPieceSize, path)
		if err != nil {
			return err
		}

		deals := []struct {
			offset storiface.PaddedByteIndex
			size   abi.PaddedPieceSize
		}{
			{ // first deal
				storiface.PaddedByteIndex(0),
				//abi.PaddedPieceSize(2147483648),
				abi.PaddedPieceSize(1125),
				//abi.PaddedPieceSize(2000011010),
				//abi.PaddedPieceSize(1023),
			},
			{ // second deal
				storiface.PaddedByteIndex(1 * 2147483648),
				abi.PaddedPieceSize(2147483648),
			},
			{ // third deal
				storiface.PaddedByteIndex(2 * 2147483648),
				abi.PaddedPieceSize(2147483648),
			},
		}

		for _, d := range deals {
			err := func() error {
				defer func(now time.Time) {
					fmt.Println("commp calc took", time.Since(now))
				}(time.Now())

				offset := d.offset
				size := d.size
				f, err := pf.Reader(offset, size)
				if err != nil {
					return err
				}

				//upr, err := fr32.NewUnpadReader(f, size.Padded())
				//if err != nil {
				//return false, xerrors.Errorf("creating unpadded reader: %w", err)
				//}

				//if _, err := io.CopyN(writer, upr, int64(size)); err != nil {
				//_ = pf.Close()
				//return false, xerrors.Errorf("reading unsealed file: %w", err)
				//}

				rdr := f

				var ww bytes.Buffer
				written, err := io.CopyN(&ww, rdr, int64(size))
				if err != nil {
					return fmt.Errorf("copy into writer: %w", err)
				}
				_ = written

				w := &writer.Writer{}
				_, err = io.CopyBuffer(w, bytes.NewReader(ww.Bytes()), make([]byte, writer.CommPBuf))
				if err != nil {
					return fmt.Errorf("copy into commp writer: %w", err)
				}

				commp, err := w.Sum()
				if err != nil {
					return fmt.Errorf("computing commP failed: %w", err)
				}

				encoder := cidenc.Encoder{Base: multibase.MustNewEncoder(multibase.Base32)}

				fmt.Println("CommP CID: ", encoder.Encode(commp.PieceCID))
				fmt.Println("Piece size: ", types.NewInt(uint64(commp.PieceSize.Unpadded().Padded())))
				fmt.Println()

				return nil
			}()

			if err != nil {
				return err
			}

			break
		}

		return nil
	},
}

func getActorAddress(ctx context.Context, cctx *cli.Context) (maddr address.Address, err error) {
	if cctx.IsSet("actor") {
		maddr, err = address.NewFromString(cctx.String("actor"))
		if err != nil {
			return maddr, err
		}
		return
	}

	minerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
	if err != nil {
		return address.Undef, err
	}
	defer closer()

	maddr, err = minerApi.ActorAddress(ctx)
	if err != nil {
		return maddr, xerrors.Errorf("getting actor address: %w", err)
	}

	return maddr, nil
}
