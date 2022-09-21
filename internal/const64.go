//go:build amd64 || arm64 || loong64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x || wasm

package internal

const (
	MaxBlockSize = MaxUint16 // maximum length of a keyfile block in bytes (must not be larger than MaxKeySize due to on-disk representation)
	MaxKeySize   = MaxUint16 // maximum length of a data record's key in bytes
	MaxDataSize  = MaxUint48 // maximum length of a data record's value in bytes
)
