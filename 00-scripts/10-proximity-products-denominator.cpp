// Open ts-yearly-data.Rproj before running this function

#include <omp.h>
#include <RcppArmadillo.h>

// [[Rcpp::depends(RcppArmadillo)]]
// [[Rcpp::plugins(openmp)]]

using namespace Rcpp;

// [[Rcpp::export]]
arma::mat proximity_products_denominator(arma::sp_mat Mcp, arma::mat U, int cores = 1) {
  // Constants
  int N = (int) Mcp.n_cols;
  
  // Output
  arma::mat Phi_down(N,N);
  
  // Filling with ones
  Phi_down.ones();
  
  // Number of cores
  omp_set_num_threads(cores);
  
#pragma omp parallel for shared(Mcp, U, N, Phi_down) default(none)
  for (int i=0; i<N; i++)
    for (int j=0; j<=i; j++) {
      // Fill the lower part
      Phi_down.at(i,j) = std::max(U(i,0), U(j,0));
      // Fill the upper part
      Phi_down.at(j,i) = Phi_down.at(i,j);
    }
    
    return Phi_down;
}
