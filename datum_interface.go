package meritop

// Datum is the interface for basic loading and transformation.
type Datum interface{}

// DatumIerator allow one to iterate through all the datum in the set.
type DatumIterator interface {
	HasNext() bool
	Next() Datum
}

// This can be used to build a sequence of Datum from source.
type DatumIteratorBuilder interface {
	Build(path string) DatumIterator
}

// Transform Datum from one format to another.
type DatumTransformer interface {
	Transform(old Datum) Datum
}

// DatumStore host a set of Datum in the memory.
type DatumStore struct {
	Cache []Datum
}
