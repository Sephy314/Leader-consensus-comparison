#!/usr/bin/env python3
"""Small statistics helpers shared by the report pipeline.

No scipy dependency: mean, median, sample standard deviation, 95% confidence
intervals (Student's t), and Pearson correlation with a t-based significance
measure. All functions are pure and operate on lists of floats.
"""
import math

# Two-tailed t critical values for 95% confidence, df = 1..30.
# df = n - 1. For df > 30 the normal approximation 1.96 is used.
_T95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365,
    8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145,
    15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080,
    22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048,
    29: 2.045, 30: 2.042,
}


def mean(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def median(vals):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    n = len(vals)
    if n % 2 == 1:
        return vals[n // 2]
    return (vals[n // 2 - 1] + vals[n // 2]) / 2


def stdev(vals):
    """Sample standard deviation (n-1)."""
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1))


def t95(df):
    if df < 1:
        return None
    if df <= 30:
        return _T95[df]
    return 1.96


def ci95(vals):
    """95% confidence interval of the mean: (lo, hi) or (None, None)."""
    vals = [v for v in vals if v is not None]
    n = len(vals)
    if n < 2:
        return None, None
    m = sum(vals) / n
    s = stdev(vals)
    t = t95(n - 1)
    if s is None or t is None:
        return None, None
    half = t * s / math.sqrt(n)
    return m - half, m + half


def pearson(xs, ys):
    """Pearson correlation coefficient, or None if undefined."""
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    n = len(pairs)
    if n < 2:
        return None
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    sxx = sum((p[0] - mx) ** 2 for p in pairs)
    syy = sum((p[1] - my) ** 2 for p in pairs)
    sxy = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    if sxx == 0 or syy == 0:
        return None
    return sxy / math.sqrt(sxx * syy)


def pearson_p(r, n):
    """Two-sided p-value for Pearson r under the null rho=0 (t distribution).

    Uses the incomplete beta function via a continued-fraction expansion
    (Numerical Recipes betai). Returns None when undefined.
    """
    if r is None or n < 3:
        return None
    if abs(r) >= 1.0:
        return 0.0
    df = n - 2
    t = r * math.sqrt(df / (1 - r * r))
    x = df / (df + t * t)
    # p = I_{x}(df/2, 1/2) for the two-sided test.
    a, b = df / 2.0, 0.5
    return betai(a, b, x)


def betacf(a, b, x, itmax=200, eps=3e-12):
    """Continued fraction for the incomplete beta function."""
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < 1e-30:
        d = 1e-30
    d = 1.0 / d
    h = d
    for m in range(1, itmax + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break
    return h


def betai(a, b, x):
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    ln = (math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
          + a * math.log(x) + b * math.log(1.0 - x))
    bt = math.exp(ln)
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * betacf(a, b, x) / a
    return 1.0 - bt * betacf(b, a, 1.0 - x) / b