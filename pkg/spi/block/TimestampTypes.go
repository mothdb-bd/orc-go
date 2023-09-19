package block

func WriteLongTimestamp(blockBuilder BlockBuilder, timestamp *LongTimestamp) {
	WriteLongTimestamp2(blockBuilder, timestamp.GetEpochMicros(), timestamp.GetPicosOfMicro())
}

func WriteLongTimestamp2(blockBuilder BlockBuilder, epochMicros int64, fraction int32) {
	blockBuilder.WriteLong(epochMicros)
	blockBuilder.WriteInt(fraction)
	blockBuilder.CloseEntry()
}
