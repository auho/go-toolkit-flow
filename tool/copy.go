package tool

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

func CopySliceSlice[E any](items [][]E) [][]E {
	newItems := make([][]E, 0, len(items))
	for _, v := range items {
		newItem := make([]E, len(v), len(v))
		_ = copy(newItem, v)

		newItems = append(newItems, newItem)
	}

	return newItems
}
