package bwmf

import ()

// The abstraction of matrix factorization solver.
type MFSolver interface {
	Solve(A, Wo, Ho Matrix) (W, H Matrix)
}

type AlternatingMFSolver interface {
	SolveSubProblem(A, W, Ho Matrix, transpose bool) (H Matrix)
}

type ObjectiveFunction interface {
}

type Optimizer interface {
}
