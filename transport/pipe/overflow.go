package pipe

// OnOverflow reports discarded buffers at the owning transport boundary.
func OnOverflow(report func(int64)) Option {
	return func(options *pipeOption) { options.onOverflow = report }
}
