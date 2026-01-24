package patcher

import "fmt"

// Repatch removes legacy/modern hints and reapplies the latest patch format.
func Repatch(sql string) (*PatchResult, error) {
	depatched, err := Depatch(sql)
	if err != nil {
		return nil, fmt.Errorf("depatch failed: %w", err)
	}
	patched, err := Patch(depatched.PatchedSQL)
	if err != nil {
		return nil, fmt.Errorf("patch failed: %w", err)
	}
	patched.Warnings = append(depatched.Warnings, patched.Warnings...)
	return patched, nil
}
