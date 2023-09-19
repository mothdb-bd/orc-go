package metadata

import "github.com/mothdb-bd/orc-go/pkg/spi/block"

type StatisticsBuilder interface {
	AddBlock(kind block.Type, block block.Block)

	BuildColumnStatistics() *ColumnStatistics
}
