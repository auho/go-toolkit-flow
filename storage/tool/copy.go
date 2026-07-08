package tool

// CopySliceMap returns a deep copy of a slice of maps.
func CopySliceMap[E any](items []map[string]E) []map[string]E {
	newItems := make([]map[string]E, 0, len(items))
	for _, v := range items {
		newItem := make(map[string]E, len(v))
		for k1, v1 := range v {
			newItem[k1] = v1
		}

		newItems = append(newItems, newItem)
	}

	return newItems
}

// CopySliceSlice returns a deep copy of a slice of slices.
func CopySliceSlice[E any](items [][]E) [][]E {
	newItems := make([][]E, 0, len(items))
	for _, v := range items {
		newItem := make([]E, len(v))
		copy(newItem, v)

		newItems = append(newItems, newItem)
	}

	return newItems
}

// CopySliceString returns a deep copy of a string slice.
func CopySliceString(items []string) []string {
	newItems := make([]string, len(items))
	copy(newItems, items)
	return newItems
}
