package game

type Region int

const (
	NA Region = iota
	AS
	EU
	NAE
	NAW
)

func (r Region) ToString() string {
	switch r {
	case NA:
		return "North America"
	case EU:
		return "Europe"
	case AS:
		return "Asia"
	case NAE:
		return "NA (East)"
	case NAW:
		return "NA (West)"
	}
	return "Unknown"
}
