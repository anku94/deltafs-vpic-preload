/*
 * Lower tail quantile for standard normal distribution function.
 *
 * This function returns an approximation of the inverse cumulative
 * standard normal distribution function.  I.e., given P, it returns
 * an approximation to the X satisfying P = Pr{Z <= X} where Z is a
 * random variable from the standard normal distribution.
 *
 * The algorithm uses a minimax approximation by rational functions
 * and the result has a relative error whose absolute value is less
 * than 1.15e-9.
 *
 * Author:      Peter John Acklam
 * Time-stamp:  2002-06-09 18:45:44 +0200
 * E-mail:      jacklam@math.uio.no
 * WWW URL:     http://www.math.uio.no/~jacklam
 *
 * C implementation adapted from Peter's Perl version
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cerrno>

#include "preload_shuffle.h"

/* Coefficients in rational approximations. */
static const double a[] =
{
	-3.969683028665376e+01,
	 2.209460984245205e+02,
	-2.759285104469687e+02,
	 1.383577518672690e+02,
	-3.066479806614716e+01,
	 2.506628277459239e+00
};

static const double b[] =
{
	-5.447609879822406e+01,
	 1.615858368580409e+02,
	-1.556989798598866e+02,
	 6.680131188771972e+01,
	-1.328068155288572e+01
};

static const double c[] =
{
	-7.784894002430293e-03,
	-3.223964580411365e-01,
	-2.400758277161838e+00,
	-2.549732539343734e+00,
	 4.374664141464968e+00,
	 2.938163982698783e+00
};

static const double d[] =
{
	7.784695709041462e-03,
	3.224671290700398e-01,
	2.445134137142996e+00,
	3.754408661907416e+00
};

#define LOW 0.02425
#define HIGH 0.97575

double
ltqnorm(double p)
{
	double q, r;

	errno = 0;

	if (p < 0 || p > 1)
	{
		errno = EDOM;
		return 0.0;
	}
	else if (p == 0)
	{
		errno = ERANGE;
		return -HUGE_VAL /* minus "infinity" */;
	}
	else if (p == 1)
	{
		errno = ERANGE;
		return HUGE_VAL /* "infinity" */;
	}
	else if (p < LOW)
	{
		/* Rational approximation for lower region */
		q = sqrt(-2*log(p));
		return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) /
			((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);
	}
	else if (p > HIGH)
	{
		/* Rational approximation for upper region */
		q  = sqrt(-2*log(1-p));
		return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) /
			((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);
	}
	else
	{
		/* Rational approximation for central region */
    		q = p - 0.5;
    		r = q*q;
		return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q /
			(((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1);
	}
}

int gaussian_buckets(double mu, double sigma, double *bucket_out, int n) {
  // assert len(bucket_out) == n
  for(int i = 0; i <= n; i++) {
    double cur = i * 1.0 / n;
    // printf("cur: %lf\n", cur);
    bucket_out[i] = ltqnorm(cur) * sigma + mu;
    // printf("out: %lf\n", ltqnorm(cur));
  }
  return 0;
}

double compute_energy(double ux, double uy, double uz) {
  double tmp = 1 + ux*ux + uy*uy + uz*uz;
  return sqrt(tmp);
}

double compute_energy(const char *data_blob) {
  float f[10];
  memcpy(f, data_blob, 40);

  double energy = compute_energy(f[5], f[6], f[7]);
  return energy;
}

int buffer_write(shuffle_ctx_t* ctx, const char* fname,
                  unsigned char fname_len, char* data, unsigned char data_len,
                  int epoch) {
  std::string fname_copy (fname, fname_len);
  std::string data_copy (data, data_len);
  ctx->temp_buffer[fname_copy] = data_copy;
  return 0;
}

int binary_search(double *buckets, int n, double energy) {
  // printf("--> Looking for %lf: ", energy);
  for(int i = n; i >= 0; i--) {
    // printf("[%d]%lf ", i, buckets[i]);
    if (buckets[i] < energy) {
      // printf("\n--> selected %d\n", i);
      return i;
    }
  }
  return -1;
}

int get_buckets(double px, double px2, double py, double py2,
    double pz, double pz2, double *buckets, long int n, int nproc) {
  printf("---> Computing gaussian buckets: nproc: %d, nelem: %ld\n", nproc, n);
  // get buckets from px/px2
  double xmu = px/n;
  double xsig = px2/n - (xmu*xmu);
  double xbuckets[nproc + 10];
  gaussian_buckets(xmu, xsig, xbuckets, nproc);

  // get buckets from py/py2
  double ymu = py/n;
  double ysig = py2/n - (ymu*ymu);
  double ybuckets[nproc + 10];
  gaussian_buckets(ymu, ysig, ybuckets, nproc);

  // get buckets from pz/pz2
  double zmu = pz/n;
  double zsig = pz2/n - (zmu*zmu);
  double zbuckets[nproc + 10];
  gaussian_buckets(zmu, zsig, zbuckets, nproc);

  // combine the 3 into arg:buckets
  buckets[0] = xbuckets[0]; // -inf
  buckets[nproc] = xbuckets[nproc]; // +inf

  for (int i = 1; i < nproc; i++) {
    buckets[i] = compute_energy(xbuckets[i], ybuckets[i], zbuckets[i]);
    printf("--> get_buckets: %lf: %lf %lf %lf\n", buckets[i],
        xbuckets[i], ybuckets[i], zbuckets[i]);
  }

  return 0;
}

