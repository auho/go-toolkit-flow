package tool

type StringEntry = string
type AnyEntry = any

type Entry interface {
	StringEntry | AnyEntry
}

func CopySliceMap[E Entry](items []map[string]E) []map[string]E {
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

func CopySliceSlice[E Entry](items [][]E) [][]E {
	newItems := make([][]E, len(items), len(items))
	for _, v := range items {
		newItem := make([]E, len(v), len(v))
		_ = copy(newItem, v)

		newItems = append(newItems, newItem)
	}

	return newItems
}
