package store

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongInputStream interface {

	// 继承

	ValueInputStream[LongStreamCheckpoint]
	Next() int64
	Next2(values []int64, items int32)
	Next3(values []int32, items int32)
	Next4(values []int16, items int32)
	Sum(items int32) int64
	// {
	//     long sum = 0;
	//     for (int i = 0; i < items; i++) {
	//         sum += next();
	//     }
	//     return sum;
	// }
}

func SumDefault(lm LongInputStream, items int32) int64 {
	sum := util.INT64_ZERO
	for i := util.INT64_ZERO; i < int64(items); i++ {
		sum += lm.Next()
	}
	return sum
}
