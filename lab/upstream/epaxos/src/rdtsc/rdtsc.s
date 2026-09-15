//go:build amd64

// func Cputicks(void) (n uint64)
TEXT ·Cputicks(SB),7,$0
    RDTSC
    SHLQ  $32, DX
    ADDQ  DX, AX
    MOVQ  AX, n+0(FP)
    RET

