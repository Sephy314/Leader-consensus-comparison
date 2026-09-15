//go:build arm64

// func Cputicks(void) (n uint64)
TEXT ·Cputicks(SB),7,$0
	MRS CNTVCT_EL0, R0
	MOVD R0, n+0(FP)
	RET
