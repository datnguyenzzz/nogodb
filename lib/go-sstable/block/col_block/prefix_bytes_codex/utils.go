package prefixbytescodex

func GetBundleFromRow(rows uint32, bundleSize byte) uint32 {
	return rows / uint32(bundleSize)
}

func GetBundlePrefixPos(rows uint32, bundleSize byte) uint32 {
	return GetBundleFromRow(rows, bundleSize)*(uint32(bundleSize)+1) + 1
}

func GetPosFromRow(rows uint32, bundleSize byte) uint32 {
	return GetBundlePrefixPos(rows, bundleSize) + rows%uint32(bundleSize) + 1
}
