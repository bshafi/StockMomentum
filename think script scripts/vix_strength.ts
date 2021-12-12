input data = close;
input length = 10;
def mvg_avg = Average(data, length);
def mvg_std = StDev(data, length);
def x = (data - mvg_avg) / (mvg_std * Sqrt(2));
def p = 0.3275911;
def a1 = 0.254829592;
def a2 = −0.284496736;
def a3 = 1.421413741;
def a4 = −1.453152027;
def a5 = 1.061405429;
def t = 1 / (1 + p * x);
def erf_x = 1 - (t * (a1 + t * (a2 + t * (a3 + t * (a4 + t * a5))))) * Exp(-1 * x * x);
plot vix_strength = 100 * (((1 + erf_x) / 2) + 1);
 
